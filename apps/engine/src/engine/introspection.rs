//! Introspection surface: the graph/state DTOs, the operator/arrangement graph builders,
//! node-state summaries, and deep node dumps.

use super::*;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ShapeRecord {
    pub id: String,
    pub table: String,
    pub stream_path: String,
    /// Graph-introspection metadata (for `GET /graph` / the pipeline visualizer). Filled at creation.
    pub changes_only: bool,
    /// The shape's `where` predicate as raw JSON, for rendering the pipeline. `None` = match-all.
    pub where_json: Option<PredicateJson>,
    /// The columns this shape projects (syncs), as requested at creation. `None` = the full row (all
    /// columns). Surfaced for the visualizer so a shape's SELECT-list is visible.
    pub columns: Option<Vec<String>>,
    /// `Some(key_cols)` iff this shape is an equality template routed by a shared **family** on those
    /// columns; `None` = standalone filter or subquery.
    pub family_key: Option<Vec<String>>,
    /// True iff the predicate contains a `col IN (SELECT …)` leaf (routed via the subquery registry).
    pub is_subquery: bool,
    /// Present iff this shape is a scalar **aggregation** (maintains a running COUNT/SUM/… over `where`,
    /// not the rows). Streams a single value that updates as rows enter/leave the predicate.
    pub aggregate: Option<AggInfo>,
}

/// Aggregation descriptor carried on a shape record + `GET /graph` (for the visualizer).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggInfo {
    pub func: AggFn,
    pub col: Option<String>,
}

// --- Pipeline-graph introspection (served at `GET /graph` for the visualizer) ---

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphShape {
    pub id: String,
    pub table: String,
    pub stream_path: String,
    pub changes_only: bool,
    #[serde(rename = "where")]
    pub where_: Option<PredicateJson>,
    /// The projected columns (SELECT-list); `null` = the full row (all columns).
    pub columns: Option<Vec<String>>,
    /// Key columns iff this shape routes via a shared equality **family**; else `null` (standalone/subquery).
    pub family_key: Option<Vec<String>>,
    pub is_subquery: bool,
    /// Present iff this shape is a scalar aggregation (COUNT/SUM/…).
    pub aggregate: Option<AggInfo>,
    /// Present iff this shape is **circuit-served** (seeded + maintained by the dbsp pipeline);
    /// says which cohort form serves it (`all` / `static:<col>` / `dynamic:<col>` / `counts`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit: Option<CircuitPlacement>,
    /// Retention lifecycle: `active` | `deactivating` | `dormant` | `reactivating` (`None` while
    /// the record is mid-create). A dormant shape keeps its stream + record but holds no routing
    /// state — the visualizer renders it parked instead of live.
    pub state: Option<&'static str>,
}

/// A shared maintained inner-set node (`SELECT proj FROM inner WHERE …`), one per distinct subquery.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphNode {
    pub sig: String,
    pub inner_table: String,
    pub proj_col: String,
    pub distinct_values: usize,
    pub refcount: usize,
}

/// A dependency edge from a subquery node to a dependent (an outer shape, or a parent node for nesting).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphEdge {
    pub node_sig: String,
    pub dependent_kind: String, // "shape" | "node"
    pub dependent_id: String,
    pub connecting_col: String,
    pub negated: bool,
}

/// One operator of the exploded circuit view: the engine's own decomposition of what it
/// executes per node, so the visualizer renders operators the engine declares instead of guessing.
/// `hop` binds the operator to the trace-hop id whose outcomes animate it; `state` (when present)
/// binds it to the state-summary id whose live chips it shows — the operator that actually holds
/// the state, and only that one.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpNode {
    pub id: String,
    /// `source | delta | filter | key | arrange | join | distinct | fold | project | sink`
    pub kind: String,
    /// Trace-hop / graph node id (`table:`, `filter:`, `family:`, `node:`, `shape:`).
    pub hop: String,
    /// State-summary id (`GET /state` key) when this operator is the state-bearing one.
    pub state: Option<String>,
    pub label: String,
}

/// A stream between two operators of the exploded circuit view.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpEdge {
    pub source: String,
    pub target: String,
    /// `flow` (a Z-set stream) | `state` (an arrangement feeding a join) | `subquery` (an
    /// inner-set membership dependency).
    pub kind: String,
    pub label: Option<String>,
}

/// One compiled table input of the dbsp arrangement circuit (id `arr:input:<table>`).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArrInput {
    pub id: String,
    pub table: String,
    /// Whether the table's initial seed completed (until then, lookups fall back to Postgres).
    pub seeded: bool,
}

/// One compiled index pipeline of the arrangement circuit — `input → map_index(cols) →
/// integrate_trace` — with id `arr:index:<table>:<col,col>` (column names, in index order).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArrIndex {
    pub id: String,
    /// The feeding table input's node id (`arr:input:<table>`) — the input→index edge.
    pub input: String,
    pub table: String,
    /// Index-key column names, in order.
    pub cols: Vec<String>,
    /// Mirrors the table input's seeded flag (an index serves iff its table is seeded).
    pub seeded: bool,
}

/// A live consumer of a compiled index: a subquery dependent whose flip re-derivations are served
/// from that index's snapshot (`query_candidates` in `subquery.rs`). Unlike the inputs/indexes —
/// which are fixed at boot — consumers appear and disappear with the shapes/nodes that need them.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArrConsumer {
    /// The serving index's node id (an `ArrIndex::id`).
    pub index: String,
    /// `"shape"` (an outer subquery shape) or `"node"` (a parent node, for nested IN).
    pub dependent_kind: String,
    /// The dependent's id in this graph: a shape id, or a subquery node signature.
    pub dependent_id: String,
    /// Column name (in the dependent's queried table) the lookup keys on.
    pub connecting_col: String,
}

/// The compiled dbsp arrangement pipeline (see `arrangements.rs` and `docs/ARCHITECTURE.md` §6b):
/// static infrastructure built once at boot, plus its live consumers and the layer's lookup
/// counters. Present in `/graph` whenever the circuit is running (always, in Postgres mode).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArrangementGraph {
    /// Lookups served from arrangement snapshots.
    pub served: u64,
    /// Lookups that fell back to Postgres (missing index, or table not seeded yet).
    pub fallback: u64,
    pub inputs: Vec<ArrInput>,
    pub indexes: Vec<ArrIndex>,
    /// Counts pipelines (`map_index(group) → weighted_count`), one per counted table.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub counts: Vec<ArrCounts>,
    pub consumers: Vec<ArrConsumer>,
}

/// One counts pipeline node in the compiled circuit.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct ArrCounts {
    /// `arr:counts:<table>`.
    pub id: String,
    /// The feeding table input's node id (the input→counts edge).
    pub input: String,
    pub table: String,
    pub group_cols: Vec<String>,
    pub seeded: bool,
}

/// The whole maintained pipeline at an instant: tables, shapes (with their routing placement),
/// the shared subquery node/edge DAG, and the exploded operator decomposition (`operators` /
/// `opEdges`) the circuit view renders. The visualizer derives family + subquery sharing from this.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineGraph {
    pub tables: Vec<String>,
    pub shapes: Vec<GraphShape>,
    pub subquery_nodes: Vec<GraphNode>,
    pub subquery_edges: Vec<GraphEdge>,
    pub operators: Vec<OpNode>,
    pub op_edges: Vec<OpEdge>,
    /// The compiled dbsp arrangement pipeline; present once the circuit is running (always, in
    /// Postgres mode), omitted only during the pre-`setup_postgres` window and in library mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arrangements: Option<ArrangementGraph>,
}

/// One column of a table's schema, as surfaced to the visualizer (`GET /table/{name}/schema`) so it
/// can render one input per column (with the pk flagged) in the add-row form.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableColumnInfo {
    pub name: String,
    /// Coarse engine type: `int` | `text` | `bool` | `float`.
    #[serde(rename = "type")]
    pub ty: &'static str,
    /// Raw Postgres type name (`udt_name`, e.g. `int4`, `uuid`, `timestamptz`); `null` in library mode.
    pub pg_type: Option<String>,
    /// Whether this column is part of the primary key.
    pub pk: bool,
    /// Whether Postgres auto-supplies the value when omitted (IDENTITY or `DEFAULT`) — the add-row
    /// form treats such columns as optional. Always `false` in library mode.
    pub has_default: bool,
}

/// A table's column list + primary key (`GET /table/{name}/schema`).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchemaInfo {
    pub table: String,
    pub columns: Vec<TableColumnInfo>,
    pub primary_key: Vec<String>,
}

/// One entry of a subquery node's live inner-set index.
#[derive(serde::Serialize)]
pub struct NodeValue {
    pub value: serde_json::Value,
    pub contributors: usize,
}

/// The live inner-set index of a subquery node (served at `GET /graph/node?sig=…`).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeIndex {
    pub sig: String,
    pub distinct_values: usize,
    pub refcount: usize,
    pub values: Vec<NodeValue>,
    pub truncated: bool,
}

/// Live state summary of one pipeline node, keyed by the node's graph/trace id (`table:<t>`,
/// `filter:<sid>`, `family:<t>:<cols>`, `node:<sig>`, `shape:<sid>`). Served in bulk at
/// `GET /state`, pushed as `{"type":"state", "nodes":{…}}` events on the `/trace` channel after
/// each processed batch, and rendered by the visualizer as per-node state chips.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum NodeStateSummary {
    /// A table source: the tailer's convergence offset + envelopes processed since start.
    #[serde(rename_all = "camelCase")]
    Table { processed_offset: String, envelopes: u64 },
    /// A standalone stateless filter (σ + π): envelopes it has emitted downstream.
    #[serde(rename_all = "camelCase")]
    Filter { emitted: u64 },
    /// A shared equality router: cardinality of its routing index (distinct key tuples) and the
    /// number of shapes registered across those keys.
    #[serde(rename_all = "camelCase")]
    Family { keys: usize, shapes: usize },
    /// A shape output stream: envelopes appended to it (backfill + live).
    #[serde(rename_all = "camelCase")]
    Shape { emitted: u64 },
    /// A scalar aggregation fold: its current value and internal fold state.
    #[serde(rename_all = "camelCase")]
    Aggregate { value: serde_json::Value, count: i64, nn_count: i64, multiset_len: usize },
    /// A shared subquery inner-set arrangement: distinct values maintained + dependent refcount.
    #[serde(rename_all = "camelCase")]
    SubqueryNode { distinct_values: usize, refcount: usize },
}

/// Full per-node state snapshot (`GET /state`) — the seed the visualizer loads before applying
/// incremental `state` events from `/trace`.
#[derive(serde::Serialize)]
pub struct StateSnapshot {
    pub nodes: HashMap<String, NodeStateSummary>,
}

/// Per-table circuit topology: the shared family circuits (one per equality template) and the
/// count of standalone per-shape circuits. Exposed via `GET /tables/{name}/families` so a test can
/// prove that many same-template shapes share one circuit rather than spawning N.
#[derive(Clone, Default, serde::Serialize)]
pub struct TableStats {
    pub families: Vec<FamilyStat>,
    pub standalone: usize,
    /// Circuit-served shapes + aggregates on this table.
    #[serde(default)]
    pub circuit: usize,
}

#[derive(Clone, serde::Serialize)]
pub struct FamilyStat {
    pub key_cols: Vec<usize>,
    pub shapes: usize,
}

/// The exploded operator decomposition of the maintained pipeline — what the engine ACTUALLY
/// executes per node, one operator box per real step, generated from the same registered
/// structures `/graph` reports. Pure over the graph pieces so it is unit-testable and provably
/// consistent with the topology: every operator's `hop` is a trace-hop id and every `state` is a
/// `GET /state` key, so the circuit view animates and shows live state with zero client guessing.
pub(crate) fn circuit_ops(
    tables: &[String],
    shapes: &[GraphShape],
    subquery_nodes: &[GraphNode],
    subquery_edges: &[GraphEdge],
) -> (Vec<OpNode>, Vec<OpEdge>) {
    let mut ops: Vec<OpNode> = Vec::new();
    let mut edges: Vec<OpEdge> = Vec::new();
    let op = |id: &str, kind: &str, hop: &str, state: Option<String>, label: &str| OpNode {
        id: id.to_string(),
        kind: kind.to_string(),
        hop: hop.to_string(),
        state,
        label: label.to_string(),
    };
    let flow = |s: &str, t: &str| OpEdge { source: s.into(), target: t.into(), kind: "flow".into(), label: None };

    // Every table: the stream tailer (source) and the envelope → Z-set delta step it runs.
    for t in tables {
        let hop = format!("table:{t}");
        ops.push(op(&format!("src:{t}"), "source", &hop, Some(hop.clone()), t));
        ops.push(op(&format!("d:{t}"), "delta", &hop, None, "Δ change"));
        edges.push(flow(&format!("src:{t}"), &format!("d:{t}")));
    }

    // Shared family operators are emitted once per (table, key-cols), like the router itself.
    let mut fams_done: HashSet<(String, String)> = HashSet::new();

    for s in shapes {
        let sid = &s.id;
        let t = &s.table;
        let d = format!("d:{t}");
        let shape_hop = format!("shape:{sid}");
        let snk_id = format!("snk:{sid}");

        if let Some(agg) = &s.aggregate {
            // apply(): σ over the delta, then the incremental fold; the sink appends on change.
            let fn_label = format!("Σ {}({})", format!("{:?}", agg.func).to_uppercase(), agg.col.as_deref().unwrap_or("*"));
            ops.push(op(&format!("sigma:{sid}"), "filter", &shape_hop, None, "σ where"));
            ops.push(op(&format!("fold:{sid}"), "fold", &shape_hop, Some(shape_hop.clone()), &fn_label));
            ops.push(op(&snk_id, "sink", &shape_hop, None, &s.stream_path));
            edges.push(flow(&d, &format!("sigma:{sid}")));
            edges.push(flow(&format!("sigma:{sid}"), &format!("fold:{sid}")));
            edges.push(flow(&format!("fold:{sid}"), &snk_id));
            continue;
        }

        if s.is_subquery {
            // The outer predicate evaluates with IN-membership against node arrangements — a
            // semijoin/antijoin; flips arrive on the subquery edges added below.
            ops.push(op(&format!("sj:{sid}"), "join", &shape_hop, None, "⋈ membership"));
            ops.push(op(&format!("pi:{sid}"), "project", &shape_hop, None, "π pk → envelope"));
            ops.push(op(&snk_id, "sink", &shape_hop, Some(shape_hop.clone()), &s.stream_path));
            edges.push(flow(&d, &format!("sj:{sid}")));
            edges.push(flow(&format!("sj:{sid}"), &format!("pi:{sid}")));
            edges.push(flow(&format!("pi:{sid}"), &snk_id));
            continue;
        }

        if let Some(key) = &s.family_key {
            let cols = key.join(",");
            let fam_hop = format!("family:{t}:{cols}");
            let (key_id, arr_id, join_id) =
                (format!("key:{t}:{cols}"), format!("arr:{t}:{cols}"), format!("rjoin:{t}:{cols}"));
            if fams_done.insert((t.clone(), cols.clone())) {
                ops.push(op(&key_id, "key", &fam_hop, None, &format!("↦ key({cols})")));
                ops.push(op(&arr_id, "arrange", &fam_hop, Some(fam_hop.clone()), "params: key → shapes"));
                ops.push(op(&join_id, "join", &fam_hop, None, "⋈ route"));
                edges.push(flow(&d, &key_id));
                edges.push(flow(&key_id, &join_id));
                edges.push(OpEdge { source: arr_id, target: join_id.clone(), kind: "state".into(), label: None });
            }
            ops.push(op(&format!("pi:{sid}"), "project", &shape_hop, None, "π pk → envelope"));
            ops.push(op(&snk_id, "sink", &shape_hop, Some(shape_hop.clone()), &s.stream_path));
            edges.push(OpEdge {
                source: join_id,
                target: format!("pi:{sid}"),
                kind: "flow".into(),
                label: Some(sid.clone()),
            });
            edges.push(flow(&format!("pi:{sid}"), &snk_id));
            continue;
        }

        // Standalone: stateless σ directly on the delta, then group-by-pk into envelopes.
        let filter_hop = format!("filter:{sid}");
        ops.push(op(&format!("sigma:{sid}"), "filter", &filter_hop, Some(filter_hop.clone()), "σ where"));
        ops.push(op(&format!("pi:{sid}"), "project", &shape_hop, None, "π pk → envelope"));
        ops.push(op(&snk_id, "sink", &shape_hop, Some(shape_hop.clone()), &s.stream_path));
        edges.push(flow(&d, &format!("sigma:{sid}")));
        edges.push(flow(&format!("sigma:{sid}"), &format!("pi:{sid}")));
        edges.push(flow(&format!("pi:{sid}"), &snk_id));
    }

    // Shared subquery inner sets: σ inner where → π projected column → distinct arrangement.
    for n in subquery_nodes {
        let sig = &n.sig;
        let hop = format!("node:{sig}");
        ops.push(op(&format!("sqf:{sig}"), "filter", &hop, None, "σ inner where"));
        ops.push(op(&format!("sqp:{sig}"), "project", &hop, None, &format!("π {}", n.proj_col)));
        ops.push(op(&format!("dist:{sig}"), "distinct", &hop, Some(hop.clone()), &format!("distinct {}", n.proj_col)));
        edges.push(flow(&format!("d:{}", n.inner_table), &format!("sqf:{sig}")));
        edges.push(flow(&format!("sqf:{sig}"), &format!("sqp:{sig}")));
        edges.push(flow(&format!("sqp:{sig}"), &format!("dist:{sig}")));
    }
    // Membership dependencies: a node's arrangement feeds each dependent's semijoin (or a parent
    // node's inner filter, for nested IN).
    for e in subquery_edges {
        let src = format!("dist:{}", e.node_sig);
        let (target, label) = if e.dependent_kind == "shape" {
            (format!("sj:{}", e.dependent_id), format!("{} · {}", if e.negated { "NOT IN" } else { "IN" }, e.connecting_col))
        } else {
            (format!("sqf:{}", e.dependent_id), format!("{} · {}", if e.negated { "NOT IN" } else { "IN" }, e.connecting_col))
        };
        edges.push(OpEdge { source: src, target, kind: "subquery".into(), label: Some(label) });
    }

    (ops, edges)
}

/// The compiled dbsp counts pipelines as graph nodes, plus their live consumers
/// (circuit-served COUNT aggregates). Row arrangements no longer exist — row data lives in
/// Postgres — so `indexes` is always empty and the lookup counters are gone (kept as zeros
/// for payload stability with the visualizer).
pub(crate) fn arrangement_graph(
    arr: &crate::arrangements::Arrangements,
    placements: &[(String, String, CircuitPlacement)],
    col_name: &impl Fn(&str, usize) -> String,
) -> ArrangementGraph {
    let input_id = |table: &str| format!("arr:input:{table}");
    let count_specs = arr.count_specs(); // sorted: deterministic node order across snapshots
    let inputs: Vec<ArrInput> = count_specs
        .iter()
        .map(|c| ArrInput {
            id: input_id(&c.table),
            table: c.table.clone(),
            seeded: arr.is_seeded(&c.table),
        })
        .collect();
    let counts: Vec<ArrCounts> = count_specs
        .iter()
        .map(|c| ArrCounts {
            id: format!("arr:counts:{}", c.table),
            input: input_id(&c.table),
            table: c.table.clone(),
            group_cols: c.group_cols.iter().map(|&g| col_name(&c.table, g)).collect(),
            seeded: arr.is_seeded(&c.table),
        })
        .collect();
    let mut consumers: Vec<ArrConsumer> = placements
        .iter()
        .filter(|(_, _, p)| p.counts)
        .map(|(id, table, _)| ArrConsumer {
            index: format!("arr:counts:{table}"),
            dependent_kind: "circuit-agg".to_string(),
            dependent_id: id.clone(),
            connecting_col: String::new(),
        })
        .collect();
    consumers.sort();
    consumers.dedup();
    ArrangementGraph { served: 0, fallback: 0, inputs, indexes: Vec::new(), counts, consumers }
}

/// Rebuild the tailer's full per-node state map from its live structures. Pure so it's unit-testable;
/// cost is O(shapes on this table) small clones, the same order as the fan-out work per batch.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_node_states(
    ts: &TableSchema,
    offset: &str,
    envelopes: u64,
    shapes: &HashMap<String, StandaloneShape>,
    families: &HashMap<Vec<usize>, KeyRouter>,
    family_of: &HashMap<String, (Vec<usize>, u64, Row)>,
    aggregates: &HashMap<String, AggShape>,
    circuit_aggs: &HashMap<String, CircuitAgg>,
    emitted: &HashMap<String, u64>,
) -> HashMap<String, NodeStateSummary> {
    let mut out = HashMap::new();
    out.insert(
        format!("table:{}", ts.name),
        NodeStateSummary::Table { processed_offset: offset.to_string(), envelopes },
    );
    let emitted_of = |path: &str| emitted.get(sid_of_path(path)).copied().unwrap_or(0);
    for (sid, s) in shapes {
        let n = emitted_of(&s.stream_path);
        out.insert(format!("filter:{sid}"), NodeStateSummary::Filter { emitted: n });
        out.insert(format!("shape:{sid}"), NodeStateSummary::Shape { emitted: n });
    }
    for (key_cols, router) in families {
        out.insert(
            family_node_id(ts, key_cols),
            NodeStateSummary::Family { keys: router.index.len(), shapes: router.member_count() },
        );
    }
    for sid in family_of.keys() {
        out.insert(
            format!("shape:{sid}"),
            NodeStateSummary::Shape { emitted: emitted.get(sid.as_str()).copied().unwrap_or(0) },
        );
    }
    for (sid, agg) in aggregates {
        out.insert(
            format!("shape:{sid}"),
            NodeStateSummary::Aggregate {
                value: agg.value(),
                count: agg.count,
                nn_count: agg.nn_count,
                multiset_len: agg.multiset.len(),
            },
        );
    }
    for (sid, agg) in circuit_aggs {
        out.insert(
            format!("shape:{sid}"),
            NodeStateSummary::Aggregate {
                value: serde_json::json!(agg.value),
                count: agg.value,
                nn_count: agg.value,
                multiset_len: 0,
            },
        );
    }
    out
}

/// Cap on entries returned by a `DumpNode` state dump (routing keys / multiset values).
pub(crate) const DUMP_CAP: usize = 500;

/// Full state dump of a family router: the routing index contents (`key tuple -> shape ids`).
pub(crate) fn dump_family_json(ts: &TableSchema, router: &KeyRouter) -> serde_json::Value {
    let mut entries: Vec<serde_json::Value> = router
        .index
        .iter()
        .take(DUMP_CAP)
        .map(|(key, routed)| {
            serde_json::json!({
                "key": key.0.iter().map(Value::to_json).collect::<Vec<_>>(),
                "shapes": routed.iter().map(|rs| format!("s{}", rs.num_id)).collect::<Vec<_>>(),
            })
        })
        .collect();
    entries.sort_by_key(|e| e["key"].to_string());
    serde_json::json!({
        "kind": "family",
        "node": family_node_id(ts, &router.key_cols),
        "keyCols": router.key_cols.iter()
            .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
            .collect::<Vec<_>>(),
        "keys": router.index.len(),
        "shapes": router.member_count(),
        "entries": entries,
        "truncated": router.index.len() > DUMP_CAP,
    })
}

/// Full state dump of an aggregation fold: running counters + the MIN/MAX multiset contents.
pub(crate) fn dump_aggregate_json(sid: &str, agg: &AggShape) -> serde_json::Value {
    let multiset: Vec<serde_json::Value> = agg
        .multiset
        .iter()
        .take(DUMP_CAP)
        .map(|(v, w)| serde_json::json!({ "value": v.to_json(), "weight": w }))
        .collect();
    serde_json::json!({
        "kind": "aggregate",
        "node": format!("shape:{sid}"),
        "func": agg.func,
        "value": agg.value(),
        "count": agg.count,
        "nnCount": agg.nn_count,
        "multisetLen": agg.multiset.len(),
        "multiset": multiset,
        "truncated": agg.multiset.len() > DUMP_CAP,
    })
}

pub(crate) fn stats_of(exec: &TableExec) -> TableStats {
    let mut fams: Vec<FamilyStat> = exec
        .families
        .iter()
        .map(|(k, f)| FamilyStat { key_cols: k.clone(), shapes: f.member_count() })
        .collect();
    fams.sort_by(|a, b| a.key_cols.cmp(&b.key_cols));
    TableStats {
        families: fams,
        standalone: exec.shapes.len(),
        circuit: exec.circuit_aggs.len(),
    }
}

/// Deep-dump one node's internal state for `GET /state/node` (see `SequencerCmd::DumpNode`).
pub(crate) fn dump_node_json(
    exec: &TableExec,
    offset: &str,
    emitted: &HashMap<String, u64>,
    node_id: &str,
) -> Option<serde_json::Value> {
    if node_id.starts_with("family:") {
        return exec
            .families
            .values()
            .find(|r| family_node_id(&exec.ts, &r.key_cols) == node_id)
            .map(|r| dump_family_json(&exec.ts, r));
    }
    if let Some(sid) = node_id.strip_prefix("shape:").or_else(|| node_id.strip_prefix("filter:")) {
        if let Some(agg) = exec.aggregates.get(sid) {
            return Some(dump_aggregate_json(sid, agg));
        }
        if exec.shapes.contains_key(sid) || exec.family_of.contains_key(sid) {
            return Some(serde_json::json!({
                "kind": if node_id.starts_with("filter:") { "filter" } else { "shape" },
                "node": node_id,
                "emitted": emitted.get(sid).copied().unwrap_or(0),
            }));
        }
        return None;
    }
    if node_id == format!("table:{}", exec.ts.name) {
        return Some(serde_json::json!({
            "kind": "table",
            "node": node_id,
            "processedOffset": offset,
            "envelopes": exec.envelopes_total,
        }));
    }
    None
}


impl Engine {
    /// Snapshot the whole maintained pipeline for the visualizer: tables, every registered shape with
    /// its routing placement (family key / standalone / subquery), the shared subquery node+edge DAG,
    /// and the exploded per-operator decomposition for the circuit view.
    pub async fn graph(&self) -> EngineGraph {
        let (tables, shapes, schemas, placements) = {
            let st = self.state.lock().await;
            // Deterministic output: a consumer diffing consecutive snapshots (the visualizer's
            // "did the structure change" check) must see byte-identical output for an unchanged
            // pipeline.
            let mut tables: Vec<String> = st.tables.keys().cloned().collect();
            tables.sort();
            let lives = self.lives.lock().unwrap();
            let life_of = |id: &str| -> Option<&'static str> {
                lives.get(id).map(|l| match l.state {
                    LifeState::Active => "active",
                    LifeState::Deactivating { .. } => "deactivating",
                    LifeState::Dormant { .. } => "dormant",
                    LifeState::Reactivating { .. } => "reactivating",
                })
            };
            let shapes: Vec<GraphShape> = st
                .shapes
                .values()
                .map(|r| GraphShape {
                    id: r.id.clone(),
                    table: r.table.clone(),
                    stream_path: r.stream_path.clone(),
                    changes_only: r.changes_only,
                    where_: r.where_json.clone(),
                    columns: r.columns.clone(),
                    family_key: r.family_key.clone(),
                    is_subquery: r.is_subquery,
                    aggregate: r.aggregate.clone(),
                    circuit: st.circuit_placement.get(&r.id).cloned(),
                    state: life_of(&r.id),
                })
                .collect();
            let mut shapes = shapes;
            shapes.sort_by_key(|s| s.id.strip_prefix('s').and_then(|n| n.parse::<u64>().ok()).unwrap_or(u64::MAX));
            let schemas: HashMap<String, TableSchema> = st.tables.clone();
            let placements: Vec<(String, String, CircuitPlacement)> = st
                .circuit_placement
                .iter()
                .filter_map(|(id, p)| {
                    st.shapes.get(id).map(|r| (id.clone(), r.table.clone(), p.clone()))
                })
                .collect();
            (tables, shapes, schemas, placements)
        };
        let col_name = |table: &str, idx: usize| -> String {
            schemas
                .get(table)
                .and_then(|ts| ts.columns.get(idx))
                .map(|(n, _)| n.clone())
                .unwrap_or_else(|| format!("col{idx}"))
        };
        let reg = self.subqueries.lock().await;
        let mut subquery_nodes: Vec<GraphNode> = reg
            .nodes
            .values()
            .map(|n| GraphNode {
                sig: n.sig.clone(),
                inner_table: n.inner_table.clone(),
                proj_col: col_name(&n.inner_table, n.proj_col),
                distinct_values: n.distinct_values(),
                refcount: n.refcount,
            })
            .collect();
        subquery_nodes.sort_by(|a, b| a.sig.cmp(&b.sig));
        // Each registry edge, resolved to (kind, id, queried table, connecting-column index):
        // the shape of a flip re-derivation, which is what the arrangement layer serves.
        let dependents: Vec<(&'static str, String, String, usize)> = reg
            .edges
            .iter()
            .map(|e| {
                let (kind, dep_id, dep_table) = match &e.dependent {
                    crate::subquery::Dependent::Shape(id) => (
                        "shape",
                        id.clone(),
                        reg.shapes.get(id).map(|s| s.outer_table.clone()).unwrap_or_default(),
                    ),
                    crate::subquery::Dependent::Node(sig) => (
                        "node",
                        sig.clone(),
                        reg.nodes.get(sig).map(|n| n.inner_table.clone()).unwrap_or_default(),
                    ),
                };
                (kind, dep_id, dep_table, e.connecting_col)
            })
            .collect();
        let subquery_edges: Vec<GraphEdge> = reg
            .edges
            .iter()
            .zip(&dependents)
            .map(|(e, (kind, dep_id, dep_table, _))| GraphEdge {
                node_sig: e.node_sig.clone(),
                dependent_kind: kind.to_string(),
                dependent_id: dep_id.clone(),
                connecting_col: col_name(dep_table, e.connecting_col),
                negated: e.negated,
            })
            .collect();
        drop(reg);
        let mut subquery_edges = subquery_edges;
        subquery_edges
            .sort_by(|a, b| (&a.node_sig, &a.dependent_kind, &a.dependent_id).cmp(&(&b.node_sig, &b.dependent_kind, &b.dependent_id)));
        let (operators, op_edges) = circuit_ops(&tables, &shapes, &subquery_nodes, &subquery_edges);
        let arrangements = self
            .arrangements
            .lock()
            .unwrap()
            .clone()
            .map(|arr| arrangement_graph(&arr, &placements, &col_name));
        EngineGraph { tables, shapes, subquery_nodes, subquery_edges, operators, op_edges, arrangements }
    }

    /// The live inner-set index of one subquery node (values + contributor counts), for the visualizer's
    /// node-detail view. `None` if the signature is unknown.
    pub async fn node_index(&self, sig: &str, cap: usize) -> Option<NodeIndex> {
        let reg = self.subqueries.lock().await;
        let (distinct_values, refcount, values, truncated) = reg.node_value_index(sig, cap)?;
        Some(NodeIndex {
            sig: sig.to_string(),
            distinct_values,
            refcount,
            values: values.into_iter().map(|(value, contributors)| NodeValue { value, contributors }).collect(),
            truncated,
        })
    }

    /// Full per-node state snapshot (`GET /state`): every tailer's published node map merged with
    /// the subquery registry's node/shape summaries. Tables with no tailer yet (no shape registered)
    /// report a default source state so the visualizer can render a chip for every graph node.
    pub async fn state_snapshot(&self) -> StateSnapshot {
        let mut nodes: HashMap<String, NodeStateSummary> = HashMap::new();
        {
            let st = self.state.lock().await;
            for name in st.tables.keys() {
                nodes.insert(
                    format!("table:{name}"),
                    NodeStateSummary::Table { processed_offset: "-1".to_string(), envelopes: 0 },
                );
            }
            if let Some(seq) = st.sequencer.as_ref()
                && let Ok(m) = seq.node_states.lock()
            {
                for (k, v) in m.iter() {
                    nodes.insert(k.clone(), v.clone());
                }
            }
        }
        for (id, s) in self.subqueries.lock().await.state_summaries() {
            nodes.insert(id, s);
        }
        StateSnapshot { nodes }
    }

    /// Deep state dump of one node (`GET /state/node?id=`): a family router's routing-index
    /// contents, an aggregate's fold internals (incl. the MIN/MAX multiset), a subquery node's
    /// inner-set index, or the summary counters for stateless nodes. `None` = unknown node id.
    pub async fn dump_node(&self, id: &str) -> Option<serde_json::Value> {
        if let Some(sig) = id.strip_prefix("node:") {
            let idx = self.node_index(sig, 500).await?;
            return Some(serde_json::json!({
                "kind": "subqueryNode",
                "node": id,
                "distinctValues": idx.distinct_values,
                "refcount": idx.refcount,
                "values": idx.values,
                "truncated": idx.truncated,
            }));
        }
        // Subquery shapes live in the registry, not in a table tailer.
        if let Some(sid) = id.strip_prefix("shape:") {
            let reg = self.subqueries.lock().await;
            if let Some(s) = reg.shapes.get(sid) {
                return Some(serde_json::json!({
                    "kind": "shape",
                    "node": id,
                    "emitted": s.emitted.load(std::sync::atomic::Ordering::Relaxed),
                }));
            }
        }
        // Everything else is owned by a table tailer; resolve the table and round-trip a dump.
        let table = if let Some(rest) = id.strip_prefix("family:") {
            rest.split(':').next().map(str::to_string)
        } else if let Some(rest) = id.strip_prefix("table:") {
            Some(rest.to_string())
        } else if let Some(sid) = id.strip_prefix("shape:").or_else(|| id.strip_prefix("filter:")) {
            self.state.lock().await.shapes.get(sid).map(|r| r.table.clone())
        } else {
            None
        }?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let st = self.state.lock().await;
            st.sequencer
                .as_ref()?
                .cmd_tx
                .send(SequencerCmd::DumpNode { table, node_id: id.to_string(), resp: tx })
                .ok()?;
        }
        rx.await.ok().flatten()
    }
}
