//! Unit tests for the engine module tree (moved verbatim from the former
//! `engine.rs` inline test module; items are reachable via `use super::*`).

use super::*;
use crate::schema::{TableDef, TableSchema};

/// The candidate set must contain every standalone shape that could match any row of the
/// delta (old or new side), and exclude shapes whose necessary conjunct fails on all rows.
#[test]
fn standalone_index_candidates() {
    let def: TableDef = serde_json::from_value(serde_json::json!({
        "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "age": {"type":"int"}, "active": {"type":"bool"} },
        "primaryKey": "id"
    }))
    .unwrap();
    let ts = TableSchema::from_def("users", &def).unwrap();
    let compile = |j: serde_json::Value| {
        Arc::new(
            CompiledPredicate::compile_opt(Some(&serde_json::from_value(j).unwrap()), &ts).unwrap(),
        )
    };
    let mut idx = StandaloneIndex::default();
    idx.insert("eq_a", &compile(serde_json::json!({"col":"name","op":"eq","value":"a"})));
    idx.insert("gt_18", &compile(serde_json::json!({"col":"age","op":"gt","value":18})));
    idx.insert("gte_18", &compile(serde_json::json!({"col":"age","op":"gte","value":18})));
    idx.insert("lt_10", &compile(serde_json::json!({"col":"age","op":"lt","value":10})));
    idx.insert("neq_b", &compile(serde_json::json!({"col":"name","op":"neq","value":"b"}))); // fallback scan

    let row = |name: &str, age: i64| {
        ts.row_from_json(
            serde_json::json!({"id":1,"name":name,"age":age,"active":true}).as_object().unwrap(),
        )
        .unwrap()
    };
    fn cand(idx: &StandaloneIndex, delta: &[Tup2<Row, ZWeight>]) -> Vec<String> {
        let mut c = idx.candidates(delta);
        c.sort();
        c
    }

    // age = 18 satisfies gte (non-strict) but not gt (strict); name 'a' hits the eq bucket;
    // the un-indexable neq shape is always a candidate.
    assert_eq!(cand(&idx, &[Tup2(row("a", 18), 1)]), vec!["eq_a", "gte_18", "neq_b"]);
    // age = 25 satisfies both lower bounds; name 'z' misses the eq bucket.
    assert_eq!(cand(&idx, &[Tup2(row("z", 25), 1)]), vec!["gt_18", "gte_18", "neq_b"]);
    // age = 5 satisfies only the upper bound.
    assert_eq!(cand(&idx, &[Tup2(row("z", 5), 1)]), vec!["lt_10", "neq_b"]);
    // An update whose OLD row matches a shape must surface it (the retraction side).
    assert_eq!(cand(&idx, &[Tup2(row("a", 18), -1), Tup2(row("z", 5), 1)]), vec![
        "eq_a", "gte_18", "lt_10", "neq_b"
    ]);
    // A NULL cell satisfies no comparison conjunct.
    let null_age = ts
        .row_from_json(serde_json::json!({"id":1,"name":null,"age":null,"active":true}).as_object().unwrap())
        .unwrap();
    assert_eq!(cand(&idx, &[Tup2(null_age, 1)]), vec!["neq_b"]);

    // Removal unindexes both indexed and fallback shapes.
    idx.remove("eq_a");
    idx.remove("neq_b");
    assert_eq!(cand(&idx, &[Tup2(row("a", 18), 1)]), vec!["gte_18"]);
}

/// A SubqueryHandle over a fresh registry, with a live propagator task (tests run in tokio).
fn test_subq() -> SubqueryHandle {
    let registry =
        Arc::new(Mutex::new(SubqueryRegistry::new(DsClient::new("http://127.0.0.1:1"), None)));
    let (flip_tx, flip_rx) = mpsc::unbounded_channel();
    let pending_flips = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let (trace_tx, _) = tokio::sync::broadcast::channel(16);
    spawn_flip_propagator(registry.clone(), flip_rx, pending_flips.clone(), trace_tx);
    SubqueryHandle { registry, flip_tx, pending_flips }
}

fn agg_shape(func: AggFn, col: Option<usize>, ts: &TableSchema) -> AggShape {
    let pred = Arc::new(CompiledPredicate::compile_opt(None, ts).unwrap());
    AggShape {
        pred,
        func,
        col,
        stream_path: "shape/s9".into(),
        gate: crate::pg::SnapshotGate::passthrough(),
        count: 0,
        nn_count: 0,
        sum: 0.0,
        multiset: std::collections::BTreeMap::new(),
        last: None,
    }
}

/// `build_node_states` yields one summary per node in the trace/graph id namespace: the table
/// source, filter+shape per standalone, the family router under its column-NAME id, family
/// member shapes, and aggregate folds with their live value.
#[test]
fn node_states_cover_every_node_kind() {
    let ts = users();
    let pred = Arc::new(
        CompiledPredicate::compile_opt(
            Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap()),
            &ts,
        )
        .unwrap(),
    );

    let mut shapes = HashMap::new();
    shapes.insert(
        "s1".to_string(),
        StandaloneShape {
            pred: pred.clone(),
            stream_path: "shape/s1".into(),
            gate: crate::pg::SnapshotGate::passthrough(),
            out_cols: None,
        },
    );
    let mut families = HashMap::new();
    let key_cols = vec![ts.column_index("active").unwrap()];
    let mut index = HashMap::new();
    index.insert(
        Row(vec![Value::Bool(true)]),
        vec![RoutedShape {
            num_id: 2,
            stream_path: "shape/s2".into(),
            gate: crate::pg::SnapshotGate::passthrough(),
            out_cols: None,
        }],
    );
    families.insert(key_cols.clone(), KeyRouter { key_cols: key_cols.clone(), index });
    let mut family_of = HashMap::new();
    family_of.insert("s2".to_string(), (key_cols, 2u64, Row(vec![Value::Bool(true)])));

    let mut aggregates = HashMap::new();
    let mut agg = agg_shape(AggFn::Count, None, &ts);
    agg.apply(&[Tup2(Row(vec![Value::Int(1), Value::Text("a".into()), Value::Bool(true)]), 1)]);
    aggregates.insert("s3".to_string(), agg);

    let mut emitted = HashMap::new();
    emitted.insert("s1".to_string(), 4u64);
    emitted.insert("s2".to_string(), 7u64);

    let circuit_shapes = HashMap::new();
    let circuit_aggs = HashMap::new();
    let m = build_node_states(
        &ts, "12", 42, &shapes, &families, &family_of, &aggregates, &circuit_shapes, &circuit_aggs, &emitted,
    );

    assert_eq!(
        m["table:users"],
        NodeStateSummary::Table { processed_offset: "12".into(), envelopes: 42 }
    );
    assert_eq!(m["filter:s1"], NodeStateSummary::Filter { emitted: 4 });
    assert_eq!(m["shape:s1"], NodeStateSummary::Shape { emitted: 4 });
    assert_eq!(m["family:users:active"], NodeStateSummary::Family { keys: 1, shapes: 1 });
    assert_eq!(m["shape:s2"], NodeStateSummary::Shape { emitted: 7 });
    match &m["shape:s3"] {
        NodeStateSummary::Aggregate { value, count, .. } => {
            assert_eq!(value, &serde_json::json!(1));
            assert_eq!(*count, 1);
        }
        other => panic!("expected aggregate summary, got {other:?}"),
    }
}

/// The exploded circuit decomposition is internally consistent: every edge endpoint is an
/// emitted operator, every hop is a trace-hop id, every `state` is a `GET /state` key, shared
/// structures (family, subquery node) are emitted once, and each strategy decomposes into its
/// real steps.
#[test]
fn circuit_ops_decompose_every_strategy() {
    let gs = |id: &str, table: &str, fam: Option<Vec<&str>>, sq: bool, agg: Option<AggFn>| GraphShape {
        circuit: None,
        id: id.into(),
        table: table.into(),
        stream_path: format!("shape/{id}"),
        changes_only: false,
        where_: None,
        columns: None,
        family_key: fam.map(|v| v.iter().map(|s| s.to_string()).collect()),
        is_subquery: sq,
        aggregate: agg.map(|func| AggInfo { func, col: None }),
        state: Some("active"),
    };
    let tables = vec!["users".to_string(), "orders".to_string()];
    let shapes = vec![
        gs("s1", "users", None, false, None),                    // standalone
        gs("s2", "users", Some(vec!["active"]), false, None),    // family member 1
        gs("s3", "users", Some(vec!["active"]), false, None),    // family member 2 (shared ops)
        gs("s4", "users", None, true, None),                     // subquery shape
        gs("s5", "users", None, false, Some(AggFn::Count)),      // aggregate
    ];
    let nodes = vec![GraphNode {
        sig: "orders|user_id|".into(),
        inner_table: "orders".into(),
        proj_col: "user_id".into(),
        distinct_values: 0,
        refcount: 1,
    }];
    let sq_edges = vec![GraphEdge {
        node_sig: "orders|user_id|".into(),
        dependent_kind: "shape".into(),
        dependent_id: "s4".into(),
        connecting_col: "id".into(),
        negated: false,
    }];
    let (ops, edges) = circuit_ops(&tables, &shapes, &nodes, &sq_edges);

    let ids: HashSet<&str> = ops.iter().map(|o| o.id.as_str()).collect();
    // Every edge endpoint exists.
    for e in &edges {
        assert!(ids.contains(e.source.as_str()), "dangling source {}", e.source);
        assert!(ids.contains(e.target.as_str()), "dangling target {}", e.target);
    }
    // Strategy decompositions.
    for want in [
        "src:users", "d:users", // table
        "sigma:s1", "pi:s1", "snk:s1", // standalone
        "key:users:active", "arr:users:active", "rjoin:users:active", "snk:s2", "snk:s3", // family
        "sj:s4", "snk:s4", // subquery shape
        "sigma:s5", "fold:s5", "snk:s5", // aggregate
        "sqf:orders|user_id|", "dist:orders|user_id|", // inner set
    ] {
        assert!(ids.contains(want), "missing operator {want}");
    }
    // Shared family ops emitted once despite two members.
    assert_eq!(ops.iter().filter(|o| o.id == "arr:users:active").count(), 1);
    // Hop ids use the trace namespace; state ids point at real summaries.
    let arr = ops.iter().find(|o| o.id == "arr:users:active").unwrap();
    assert_eq!(arr.hop, "family:users:active");
    assert_eq!(arr.state.as_deref(), Some("family:users:active"));
    let fold = ops.iter().find(|o| o.id == "fold:s5").unwrap();
    assert_eq!(fold.hop, "shape:s5");
    assert_eq!(fold.state.as_deref(), Some("shape:s5"));
    let sigma1 = ops.iter().find(|o| o.id == "sigma:s1").unwrap();
    assert_eq!(sigma1.hop, "filter:s1");
    // The membership edge lands on the dependent's semijoin, dashed as a subquery stream.
    let dep = edges.iter().find(|e| e.source == "dist:orders|user_id|").unwrap();
    assert_eq!(dep.target, "sj:s4");
    assert_eq!(dep.kind, "subquery");
    // The params arrangement feeds the route join as a state edge.
    assert!(edges.iter().any(|e| e.source == "arr:users:active" && e.target == "rjoin:users:active" && e.kind == "state"));
}

/// With the dbsp layer off, `/graph` omits the `arrangements` section entirely: no arr nodes
/// for the visualizer, and an unchanged payload for older consumers.
#[tokio::test]
async fn graph_omits_arrangements_when_off() {
    let engine = Engine::new(DsClient::new("http://127.0.0.1:1"));
    let g = engine.graph().await;
    assert!(g.arrangements.is_none());
    let v = serde_json::to_value(&g).unwrap();
    assert!(v.get("arrangements").is_none(), "arrangements key must be absent: {v}");
}

/// With the dbsp layer running, `/graph` carries the compiled pipeline — one input per table,
/// one index pipeline per spec (stable ids using column NAMES), seeded flags, lookup
/// counters — and connects each registered subquery dependent to the index that serves its
/// flip re-derivations. Dependents without a matching index yield no consumer, and seeding
/// flips the flags on the next snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn graph_includes_arrangement_pipeline_and_consumers() {
    let ts = users(); // columns sorted: active(0), id(1), name(2); pk = id
    let dir = std::env::temp_dir().join(format!("arr-graph-test-{}", uuid::Uuid::new_v4()));
    let arr = crate::arrangements::Arrangements::start(
        crate::arrangements::ArrangementsConfig {
            dir: dir.clone(),
            checkpoint_every: None,
            ..crate::arrangements::ArrangementsConfig::default()
        },
        vec![
            crate::arrangements::IndexSpec { table: "users".into(), cols: vec![1] }, // pk (id)
            crate::arrangements::IndexSpec { table: "users".into(), cols: vec![2] }, // name
        ],
        vec![],
    )
    .unwrap();

    let engine = Engine::new(DsClient::new("http://127.0.0.1:1"));
    engine.state.lock().await.tables.insert("users".into(), ts.clone());
    *engine.arrangements.lock().unwrap() = Some(arr.clone());

    // Register one subquery node, one dependent shape, and three edges: two on indexed
    // columns (name → shape, id → parent node) and one on an unindexed column (active).
    {
        let mut reg = engine.subqueries.lock().await;
        let sig: crate::predicate::SubquerySig = "users|name|".into();
        let pred = Arc::new(CompiledPredicate::compile_opt(None, &ts).unwrap());
        let mut node = crate::subquery::SubqueryNode::new(sig.clone(), "users".into(), 2, 1, pred.clone());
        node.refcount = 1;
        reg.nodes.insert(sig.clone(), node);
        reg.shapes.insert(
            "s1".into(),
            crate::subquery::SubqueryShape {
                shape_id: "s1".into(),
                outer_table: "users".into(),
                stream_path: "shape/s1".into(),
                pred,
                out_cols: None,
                gate: crate::pg::SnapshotGate::passthrough(),
                emitted: std::sync::atomic::AtomicU64::new(0),
            },
        );
        let edge = |dependent, connecting_col| crate::subquery::Edge {
            node_sig: sig.clone(),
            dependent,
            connecting_col,
            negated: false,
            null_sensitive: false,
        };
        reg.edges.push(edge(crate::subquery::Dependent::Shape("s1".into()), 2)); // name: indexed
        reg.edges.push(edge(crate::subquery::Dependent::Node(sig.clone()), 1)); // id: indexed
        reg.edges.push(edge(crate::subquery::Dependent::Shape("s1".into()), 0)); // active: not indexed
    }

    let g = engine.graph().await;
    let a = g.arrangements.as_ref().expect("arrangements section present");
    assert_eq!(a.inputs.len(), 1);
    assert_eq!(a.inputs[0].id, "arr:input:users");
    assert!(!a.inputs[0].seeded, "unseeded until finish_seed");
    let index_ids: Vec<&str> = a.indexes.iter().map(|i| i.id.as_str()).collect();
    assert_eq!(index_ids, vec!["arr:index:users:id", "arr:index:users:name"]);
    assert!(a.indexes.iter().all(|i| i.input == "arr:input:users" && !i.seeded));
    assert_eq!(a.indexes[1].cols, vec!["name".to_string()]);
    // Exactly the two indexed dependents become consumers (sorted by index id).
    assert_eq!(a.consumers.len(), 2, "unindexed 'active' edge must not appear: {:?}", a.consumers);
    assert_eq!(a.consumers[0].index, "arr:index:users:id");
    assert_eq!(a.consumers[0].dependent_kind, "node");
    assert_eq!(a.consumers[1].index, "arr:index:users:name");
    assert_eq!(a.consumers[1].dependent_kind, "shape");
    assert_eq!(a.consumers[1].dependent_id, "s1");
    assert_eq!(a.consumers[1].connecting_col, "name");
    // Wire format: camelCase keys under the `arrangements` section.
    let v = serde_json::to_value(&g).unwrap();
    assert_eq!(v["arrangements"]["indexes"][1]["id"], "arr:index:users:name");
    assert_eq!(v["arrangements"]["consumers"][1]["dependentKind"], "shape");
    assert_eq!(v["arrangements"]["consumers"][1]["connectingCol"], "name");
    assert_eq!(v["arrangements"]["served"], 0);
    assert_eq!(v["arrangements"]["fallback"], 0);

    // Seed the table: the next snapshot reports seeded (and served counts a lookup).
    arr.seed_chunk("users", vec![Row(vec![Value::Bool(true), Value::Int(1), Value::Text("a".into())])])
        .await
        .unwrap();
    arr.finish_seed("users");
    assert!(arr.lookup("users", &[2], &Row(vec![Value::Text("a".into())])).is_some());
    let g2 = engine.graph().await;
    let a2 = g2.arrangements.as_ref().unwrap();
    assert!(a2.inputs[0].seeded && a2.indexes.iter().all(|i| i.seeded));
    assert_eq!(a2.served, 1);

    arr.shutdown().await;
    std::fs::remove_dir_all(&dir).ok();
}

/// Wire format: summaries are kind-tagged camelCase objects, and a `StateEvent` wraps them
/// under `{"type":"state","nodes":{…}}` (the tag the visualizer switches on).
#[test]
fn state_summary_and_event_serialize_kind_tagged() {
    let s = NodeStateSummary::Aggregate {
        value: serde_json::json!(3.5),
        count: 4,
        nn_count: 2,
        multiset_len: 2,
    };
    let v = serde_json::to_value(&s).unwrap();
    assert_eq!(v["kind"], "aggregate");
    assert_eq!(v["nnCount"], 2);
    assert_eq!(v["multisetLen"], 2);

    let mut nodes = HashMap::new();
    nodes.insert("shape:s1".to_string(), NodeStateSummary::Shape { emitted: 9 });
    let ev = serde_json::to_value(crate::trace::StateEvent::new(nodes)).unwrap();
    assert_eq!(ev["type"], "state");
    assert_eq!(ev["nodes"]["shape:s1"]["kind"], "shape");
    assert_eq!(ev["nodes"]["shape:s1"]["emitted"], 9);
}

/// Deep dumps: a family router dumps its routing index (key tuple -> shape ids); a MIN/MAX
/// aggregate dumps its fold internals including the retraction multiset.
#[test]
fn dump_node_family_and_aggregate() {
    let ts = users();
    let mut index = HashMap::new();
    index.insert(
        Row(vec![Value::Bool(true)]),
        vec![RoutedShape {
            num_id: 5,
            stream_path: "shape/s5".into(),
            gate: crate::pg::SnapshotGate::passthrough(),
            out_cols: None,
        }],
    );
    let router = KeyRouter { key_cols: vec![ts.column_index("active").unwrap()], index };
    let v = dump_family_json(&ts, &router);
    assert_eq!(v["kind"], "family");
    assert_eq!(v["node"], "family:users:active");
    assert_eq!(v["keyCols"][0], "active");
    assert_eq!(v["entries"][0]["key"][0], true);
    assert_eq!(v["entries"][0]["shapes"][0], "s5");
    assert_eq!(v["truncated"], false);

    let mut agg = agg_shape(AggFn::Max, Some(0), &ts);
    agg.apply(&[
        Tup2(Row(vec![Value::Int(7), Value::Text("a".into()), Value::Bool(true)]), 1),
        Tup2(Row(vec![Value::Int(3), Value::Text("b".into()), Value::Bool(true)]), 1),
    ]);
    let v = dump_aggregate_json("s9", &agg);
    assert_eq!(v["kind"], "aggregate");
    assert_eq!(v["value"], 7);
    assert_eq!(v["count"], 2);
    assert_eq!(v["multisetLen"], 2);
    assert_eq!(v["multiset"][0]["value"], 3);
    assert_eq!(v["multiset"][0]["weight"], 1);
}

/// Planner coverage: which predicates are circuit-servable, and how they decompose.
#[tokio::test(flavor = "multi_thread")]
async fn circuit_shape_planner() {
    let ts = users(); // columns sorted: active(0), id(1), name(2); pk = id
    let members: TableSchema = {
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "user_id": {"type":"int"}, "proj": {"type":"int"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def("members", &def).unwrap()
    };
    let dir = std::env::temp_dir().join(format!("plan-test-{}", uuid::Uuid::new_v4()));
    let arr = crate::arrangements::Arrangements::start(
        crate::arrangements::ArrangementsConfig {
            dir: dir.clone(),
            checkpoint_every: None,
            ..crate::arrangements::ArrangementsConfig::default()
        },
        vec![
            crate::arrangements::IndexSpec { table: "users".into(), cols: vec![1] }, // id (pk)
            crate::arrangements::IndexSpec { table: "users".into(), cols: vec![2] }, // name
            crate::arrangements::IndexSpec { table: "members".into(), cols: vec![ *members.index.get("user_id").unwrap() ] },
        ],
        vec![],
    )
    .unwrap();
    let mut schemas = HashMap::new();
    schemas.insert("users".to_string(), ts.clone());
    schemas.insert("members".to_string(), members.clone());
    let p = |j: serde_json::Value| -> PredicateJson { serde_json::from_value(j).unwrap() };

    // match-all and static equality stay on the legacy tiers (KeyRouter/standalone route
    // them by index; a circuit shape would scan linearly per delta): no plan.
    assert!(plan_circuit_shape(None, &ts, &schemas, &arr).is_none());
    assert!(plan_circuit_shape(
        Some(&p(serde_json::json!({"col":"name","op":"eq","value":"a"}))),
        &ts, &schemas, &arr,
    )
    .is_none());
    assert!(plan_circuit_shape(
        Some(&p(serde_json::json!({"and":[
            {"or":[{"col":"name","op":"eq","value":"a"},{"col":"name","op":"eq","value":"b"}]},
            {"col":"active","op":"eq","value":true}
        ]}))),
        &ts, &schemas, &arr,
    )
    .is_none());

    // single-level dynamic IN over indexed columns
    let plan = plan_circuit_shape(
        Some(&p(serde_json::json!({"col":"name","in":{"table":"members","project":"proj",
            "where":{"col":"user_id","op":"eq","value":7}}}))),
        &ts, &schemas, &arr,
    )
    .unwrap();
    match &plan.constraint {
        PlannedConstraint::Dynamic { col, inner_table, inner_key, .. } => {
            assert_eq!(*col, 2);
            assert_eq!(inner_table, "members");
            assert_eq!(inner_key, &Value::Int(7));
        }
        other => panic!("expected Dynamic, got {other:?}"),
    }

    // dynamic IN + residual conjuncts (the board/search template): Dynamic + residual
    let plan = plan_circuit_shape(
        Some(&p(serde_json::json!({"and":[
            {"col":"name","in":{"table":"members","project":"proj",
                "where":{"col":"user_id","op":"eq","value":7}}},
            {"col":"active","op":"eq","value":true}
        ]}))),
        &ts, &schemas, &arr,
    )
    .unwrap();
    assert!(matches!(plan.constraint, PlannedConstraint::Dynamic { .. }));
    assert!(plan.residual.is_some());

    // negated IN → registry fallback
    assert!(plan_circuit_shape(
        Some(&p(serde_json::json!({"col":"name","negated":true,"in":{"table":"members","project":"proj",
            "where":{"col":"user_id","op":"eq","value":7}}}))),
        &ts, &schemas, &arr,
    )
    .is_none());

    // nested IN (inner where is itself a subquery) → registry fallback
    assert!(plan_circuit_shape(
        Some(&p(serde_json::json!({"col":"name","in":{"table":"members","project":"proj",
            "where":{"col":"proj","in":{"table":"members","project":"proj",
                "where":{"col":"user_id","op":"eq","value":1}}}}}))),
        &ts, &schemas, &arr,
    )
    .is_none());

    // two IN leaves → fallback (the constraint slot takes one; the second cannot be residual)
    assert!(plan_circuit_shape(
        Some(&p(serde_json::json!({"and":[
            {"col":"name","in":{"table":"members","project":"proj","where":{"col":"user_id","op":"eq","value":1}}},
            {"col":"name","in":{"table":"members","project":"proj","where":{"col":"user_id","op":"eq","value":2}}}
        ]}))),
        &ts, &schemas, &arr,
    )
    .is_none());

    arr.shutdown().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn circuit_agg_planner() {
    let ts = users(); // active(0), id(1), name(2)
    let group_cols = vec![2usize, 0usize]; // (name, active)
    let p = |j: serde_json::Value| -> PredicateJson { serde_json::from_value(j).unwrap() };

    // unconstrained: all dims None
    let c = plan_circuit_agg(None, &ts, &group_cols).unwrap();
    assert_eq!(c, vec![None, None]);

    // eq on one group col + IN-list on the other
    let c = plan_circuit_agg(
        Some(&p(serde_json::json!({"and":[
            {"col":"active","op":"eq","value":true},
            {"or":[{"col":"name","op":"eq","value":"a"},{"col":"name","op":"eq","value":"b"}]}
        ]}))),
        &ts, &group_cols,
    )
    .unwrap();
    assert_eq!(c[0].as_ref().unwrap().len(), 2); // name ∈ {a,b}
    assert!(c[1].as_ref().unwrap().contains(&Value::Bool(true)));

    // a non-group column → not servable
    assert!(plan_circuit_agg(
        Some(&p(serde_json::json!({"col":"id","op":"eq","value":1}))),
        &ts, &group_cols,
    )
    .is_none());

    // a non-decomposable op → not servable
    assert!(plan_circuit_agg(
        Some(&p(serde_json::json!({"col":"name","op":"like","value":"a%"}))),
        &ts, &group_cols,
    )
    .is_none());
}

fn users() -> TableSchema {
    let def: TableDef = serde_json::from_value(serde_json::json!({
        "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "active": {"type":"bool"} },
        "primaryKey": "id"
    }))
    .unwrap();
    TableSchema::from_def("users", &def).unwrap()
}

fn env(op: &str, key: &str, value: Option<serde_json::Value>, old: Option<serde_json::Value>) -> Envelope {
    Envelope {
        type_: "users".into(),
        key: key.into(),
        value,
        old,
        headers: EnvelopeHeaders { operation: op.into(), txid: None, offset: None, lsn: None, seq: None },
    }
}

/// End-to-end (sans HTTP): replication envelope (old+new) -> input delta -> direct filter eval ->
/// output envelopes, exercising enter / update / leave for a `WHERE active = true` shape.
#[test]
fn change_to_shape_envelope_enter_update_leave() {
    let ts = users();
    let pred = CompiledPredicate::compile_opt(
        Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap()),
        &ts,
    ).unwrap();

    // enter: insert an active row -> upsert envelope
    let (delta, _, _) = apply_envelope(&ts, &env("insert", "1", Some(serde_json::json!({"id":1,"name":"a","active":true})), None)).unwrap();
    let envs = translate_output(&ts, eval_standalone(&pred, &delta), None, None, None);
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].headers.operation, "upsert");
    assert_eq!(envs[0].key, "1");

    // update within shape (name change, still active) -> upsert with new value
    let (delta, _, _) = apply_envelope(&ts, &env("update", "1", Some(serde_json::json!({"id":1,"name":"a2","active":true})), Some(serde_json::json!({"id":1,"name":"a","active":true})))).unwrap();
    let envs = translate_output(&ts, eval_standalone(&pred, &delta), None, None, None);
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].headers.operation, "upsert");
    assert_eq!(envs[0].value.as_ref().unwrap()["name"], "a2");

    // leave: becomes inactive -> delete envelope
    let (delta, _, _) = apply_envelope(&ts, &env("update", "1", Some(serde_json::json!({"id":1,"name":"a2","active":false})), Some(serde_json::json!({"id":1,"name":"a2","active":true})))).unwrap();
    let envs = translate_output(&ts, eval_standalone(&pred, &delta), None, None, None);
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].headers.operation, "delete");
    assert_eq!(envs[0].key, "1");

    // a non-matching insert produces no shape envelope
    let (delta, _, _) = apply_envelope(&ts, &env("insert", "2", Some(serde_json::json!({"id":2,"name":"b","active":false})), None)).unwrap();
    let envs = translate_output(&ts, eval_standalone(&pred, &delta), None, None, None);
    assert_eq!(envs.len(), 0);
}

/// The commit LSN is stamped onto output envelopes (upsert + delete) so a subset client can
/// position its live tail at the page snapshot (see `docs/ARCHITECTURE.md` §7).
#[test]
fn translate_output_stamps_commit_lsn() {
    let ts = users();
    // upsert path: a positive-weight row carries the commit LSN.
    let out = vec![(Row(vec![crate::value::Value::Int(1), crate::value::Value::Text("a".into()), crate::value::Value::Bool(true)]), 1)];
    let envs = translate_output(&ts, out, Some("tx1".into()), Some("0/2A".into()), None);
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].headers.operation, "upsert");
    assert_eq!(envs[0].headers.lsn.as_deref(), Some("0/2A"));

    // delete path (purely negative weight) also carries the LSN.
    let out = vec![(Row(vec![crate::value::Value::Int(2), crate::value::Value::Text("b".into()), crate::value::Value::Bool(true)]), -1)];
    let envs = translate_output(&ts, out, None, Some("0/2A".into()), None);
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].headers.operation, "delete");
    assert_eq!(envs[0].headers.lsn.as_deref(), Some("0/2A"));

    // no LSN (backfill / library mode) -> none stamped.
    let out = vec![(Row(vec![crate::value::Value::Int(3), crate::value::Value::Text("c".into()), crate::value::Value::Bool(true)]), 1)];
    let envs = translate_output(&ts, out, None, None, None);
    assert_eq!(envs[0].headers.lsn, None);
}

/// The per-envelope trace reports the actual route: a family router hop (with the key) + the
/// reached shape for a key match, a `dropped` family hop when no key matches, and a `dropped`
/// filter hop for a standalone predicate that matches nothing.
#[tokio::test]
async fn trace_family_route_and_filter_drop() {
    let ts = users();
    // Columns are stored sorted: active(0), id(1), name(2).
    let name_idx = 2usize;

    // One family router on (name) with a single shape s7 registered on key 'a'.
    let mut families: HashMap<Vec<usize>, KeyRouter> = HashMap::new();
    let mut index: HashMap<Row, Vec<RoutedShape>> = HashMap::new();
    index.insert(
        Row(vec![Value::Text("a".into())]),
        vec![RoutedShape {
            num_id: 7,
            stream_path: "shape/s7".into(),
            gate: crate::pg::SnapshotGate::passthrough(),
            out_cols: None,
        }],
    );
    families.insert(vec![name_idx], KeyRouter { key_cols: vec![name_idx], index });

    // One standalone filter shape s9 whose predicate (active = false) won't match the inserts.
    let mut shapes: HashMap<String, StandaloneShape> = HashMap::new();
    shapes.insert(
        "s9".into(),
        StandaloneShape {
            pred: Arc::new(
                CompiledPredicate::compile_opt(
                    Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":false})).unwrap()),
                    &ts,
                )
                .unwrap(),
            ),
            stream_path: "shape/s9".into(),
            gate: crate::pg::SnapshotGate::passthrough(),
            out_cols: None,
        },
    );

    let mut shape_index = StandaloneIndex::default();
    let agg_index = StandaloneIndex::default();
    shape_index.insert("s9", &shapes["s9"].pred);

    let mut aggregates: HashMap<String, AggShape> = HashMap::new();
    let subqueries = test_subq();
    let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(16);
    let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();

    // Insert routed to key 'a' -> family hop routed with the key, shape s7 reached, filter s9 drops.
    process_envelope(
        &ts, &shapes, &shape_index, &families, &mut aggregates, &agg_index,
        env("insert", "1", Some(serde_json::json!({"id":1,"name":"a","active":true})), None),
        &mut pending, &subqueries, &trace_tx,
    )
    .await
    .unwrap();
    let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
    assert_eq!(ev["table"], "users");
    let hops = ev["hops"].as_array().unwrap();
    let hop = |node: &str| hops.iter().find(|h| h["node"] == node).unwrap_or_else(|| panic!("missing hop {node}: {hops:?}"));
    assert_eq!(hop("table:users")["outcome"], "passed");
    assert_eq!(hop("family:users:name")["outcome"], "routed");
    assert_eq!(hop("family:users:name")["key"][0], "a");
    assert_eq!(hop("shape:s7")["outcome"], "passed");
    assert_eq!(hop("filter:s9")["outcome"], "dropped");
    assert_eq!(ev["shapes"].as_array().unwrap(), &vec![serde_json::json!("s7")]);
    assert_eq!(ev["delta"][0]["w"], 1);
    assert_eq!(ev["delta"][0]["row"]["name"], "a");

    // Insert whose key matches no routed shape -> family hop dropped, no shapes reached.
    process_envelope(
        &ts, &shapes, &shape_index, &families, &mut aggregates, &agg_index,
        env("insert", "2", Some(serde_json::json!({"id":2,"name":"zzz","active":true})), None),
        &mut pending, &subqueries, &trace_tx,
    )
    .await
    .unwrap();
    let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
    let hops = ev["hops"].as_array().unwrap();
    let hop = |node: &str| hops.iter().find(|h| h["node"] == node).unwrap_or_else(|| panic!("missing hop {node}: {hops:?}"));
    assert_eq!(hop("family:users:name")["outcome"], "dropped");
    assert_eq!(hop("filter:s9")["outcome"], "dropped");
    assert!(ev["shapes"].as_array().unwrap().is_empty());

    // Nobody subscribed -> nothing is built or sent (receiver dropped).
    drop(trace_rx);
    process_envelope(
        &ts, &shapes, &shape_index, &families, &mut aggregates, &agg_index,
        env("insert", "3", Some(serde_json::json!({"id":3,"name":"a","active":true})), None),
        &mut pending, &subqueries, &trace_tx,
    )
    .await
    .unwrap();
    assert_eq!(trace_tx.receiver_count(), 0);
}

/// An aggregation shape appears in the trace as a `folded` hop when the delta moves its value,
/// and `dropped` when the delta doesn't match its predicate.
#[tokio::test]
async fn trace_aggregate_fold() {
    let ts = users();
    let shapes: HashMap<String, StandaloneShape> = HashMap::new();
    let shape_index = StandaloneIndex::default();
    let agg_index = StandaloneIndex::default();
    let families: HashMap<Vec<usize>, KeyRouter> = HashMap::new();
    let mut aggregates: HashMap<String, AggShape> = HashMap::new();
    aggregates.insert("s4".into(), agg(AggFn::Count, None)); // COUNT(*) WHERE active = true
    let subqueries = test_subq();
    let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(16);
    let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();

    process_envelope(
        &ts, &shapes, &shape_index, &families, &mut aggregates, &agg_index,
        env("insert", "1", Some(serde_json::json!({"id":1,"name":"a","active":true})), None),
        &mut pending, &subqueries, &trace_tx,
    )
    .await
    .unwrap();
    let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
    let hops = ev["hops"].as_array().unwrap();
    assert!(hops.iter().any(|h| h["node"] == "shape:s4" && h["outcome"] == "folded"), "{hops:?}");
    assert_eq!(ev["shapes"].as_array().unwrap(), &vec![serde_json::json!("s4")]);

    process_envelope(
        &ts, &shapes, &shape_index, &families, &mut aggregates, &agg_index,
        env("insert", "2", Some(serde_json::json!({"id":2,"name":"b","active":false})), None),
        &mut pending, &subqueries, &trace_tx,
    )
    .await
    .unwrap();
    let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
    let hops = ev["hops"].as_array().unwrap();
    assert!(hops.iter().any(|h| h["node"] == "shape:s4" && h["outcome"] == "dropped"), "{hops:?}");
}

/// A circuit-served COUNT is maintained over the table's counts pipeline, not the in-engine
/// fold, so `apply_count_deltas` must emit the fold's trace hop itself — otherwise a
/// count-affecting change would flash the source but never the fold node (and the source→fold
/// serving edge would never pulse in the visualizer's circuit view).
#[test]
fn count_delta_emits_fold_trace() {
    use crate::arrangements::CountDelta;
    let mut execs: HashMap<String, TableExec> = HashMap::new();
    let mut exec = TableExec::new(users());
    // COUNT(*) over the whole table: one unconstrained group dimension matches every group.
    exec.circuit_aggs
        .insert("s4".into(), CircuitAgg { stream_path: "shape/s4".into(), constraints: vec![None], value: 0 });
    execs.insert("users".into(), exec);

    let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(16);
    let group = |g: &str| Row(vec![Value::Text(g.into())]);
    let hop_outcome = |ev: &serde_json::Value, node: &str| {
        ev["hops"].as_array().unwrap().iter().find(|h| h["node"] == node).map(|h| h["outcome"].clone())
    };

    // Insert (+1): the fold absorbs the change; trace flags the source passed and shape:s4 folded.
    let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();
    apply_count_deltas(
        &mut execs,
        vec![CountDelta { table: "users".into(), group: group("open"), delta: 1 }],
        Some("7".into()), None, &mut pending, &trace_tx,
    );
    assert_eq!(execs["users"].circuit_aggs["s4"].value, 1);
    assert!(pending.contains_key("shape/s4"), "aggregate envelope emitted");
    let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
    assert_eq!(ev["table"], "users");
    assert_eq!(hop_outcome(&ev, "table:users"), Some(serde_json::json!("passed")));
    assert_eq!(hop_outcome(&ev, "shape:s4"), Some(serde_json::json!("folded")));
    assert_eq!(ev["delta"][0]["w"], 1);
    assert_eq!(ev["shapes"].as_array().unwrap(), &vec![serde_json::json!("s4")]);

    // Delete (−1): the dot is labelled/coloured by the negative net change.
    let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();
    apply_count_deltas(
        &mut execs,
        vec![CountDelta { table: "users".into(), group: group("open"), delta: -1 }],
        None, None, &mut pending, &trace_tx,
    );
    assert_eq!(execs["users"].circuit_aggs["s4"].value, 0);
    let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
    assert_eq!(ev["delta"][0]["w"], -1);
    assert_eq!(hop_outcome(&ev, "shape:s4"), Some(serde_json::json!("folded")));

    // Net-zero (a row moved between groups, count unchanged): no fold trace — nothing to show.
    let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();
    apply_count_deltas(
        &mut execs,
        vec![
            CountDelta { table: "users".into(), group: group("a"), delta: 1 },
            CountDelta { table: "users".into(), group: group("b"), delta: -1 },
        ],
        None, None, &mut pending, &trace_tx,
    );
    assert_eq!(execs["users"].circuit_aggs["s4"].value, 0);
    assert!(trace_rx.try_recv().is_err(), "net-zero change emits no fold trace");
}

fn agg(func: AggFn, col: Option<usize>) -> AggShape {
    let ts = users();
    let pred = Arc::new(
        CompiledPredicate::compile_opt(
            Some(&serde_json::from_value(serde_json::json!({ "col": "active", "op": "eq", "value": true })).unwrap()),
            &ts,
        )
        .unwrap(),
    );
    AggShape {
        pred,
        func,
        col,
        stream_path: "x".into(),
        gate: crate::pg::SnapshotGate::passthrough(),
        count: 0,
        nn_count: 0,
        sum: 0.0,
        multiset: std::collections::BTreeMap::new(),
        last: None,
    }
}
// Columns are stored sorted: active(0), id(1), name(2).
fn active(id: i64) -> Row {
    Row(vec![Value::Bool(true), Value::Int(id), Value::Text("n".into())])
}
fn inactive(id: i64) -> Row {
    Row(vec![Value::Bool(false), Value::Int(id), Value::Text("n".into())])
}

/// COUNT over `active = true`, maintained incrementally through inserts, deletes, and predicate-
/// crossing updates (a row moving in/out of the filter).
#[test]
fn aggregate_count_incremental() {
    let mut a = agg(AggFn::Count, None);
    a.apply(&vec![Tup2(active(1), 1), Tup2(active(2), 1), Tup2(inactive(3), 1)]);
    assert_eq!(a.value(), serde_json::json!(2)); // only the two active rows count

    a.apply(&vec![Tup2(active(1), -1), Tup2(active(4), 1)]); // one leaves, one enters
    assert_eq!(a.value(), serde_json::json!(2));

    a.apply(&vec![Tup2(inactive(3), -1), Tup2(active(3), 1)]); // update: crosses INTO the filter
    assert_eq!(a.value(), serde_json::json!(3));

    a.apply(&vec![Tup2(active(2), -1), Tup2(inactive(2), 1)]); // update: crosses OUT of the filter
    assert_eq!(a.value(), serde_json::json!(2));
}

/// SQL NULL semantics: aggregates ignore NULL values — COUNT(col) counts non-NULLs (COUNT(*)
/// counts rows), AVG divides by the non-NULL count, MIN/MAX never surface NULL, and SUM/AVG over
/// zero non-NULL values are NULL. Mirrors Postgres.
#[test]
fn aggregate_null_semantics() {
    // Columns sorted: active(0), id(1), name(2). A row with a NULL name / NULL id.
    let null_name = |id: i64| Row(vec![Value::Bool(true), Value::Int(id), Value::Null]);
    let null_id = Row(vec![Value::Bool(true), Value::Null, Value::Text("n".into())]);

    // COUNT(*) counts all matching rows; COUNT(name) only rows with non-NULL name.
    let mut star = agg(AggFn::Count, None);
    star.apply(&vec![Tup2(active(1), 1), Tup2(null_name(2), 1)]);
    assert_eq!(star.value(), serde_json::json!(2));
    let mut cnt_col = agg(AggFn::Count, Some(2));
    cnt_col.apply(&vec![Tup2(active(1), 1), Tup2(null_name(2), 1)]);
    assert_eq!(cnt_col.value(), serde_json::json!(1));

    // AVG over id where one row's aggregated column is NULL: denominator excludes it.
    let mut avg = agg(AggFn::Avg, Some(1));
    avg.apply(&vec![Tup2(active(10), 1), Tup2(active(20), 1), Tup2(null_id.clone(), 1)]);
    assert_eq!(avg.value(), serde_json::json!(15.0));

    // MIN ignores NULLs (never surfaces NULL as the extreme).
    let mut min = agg(AggFn::Min, Some(1));
    min.apply(&vec![Tup2(active(5), 1), Tup2(null_id.clone(), 1)]);
    assert_eq!(min.value(), serde_json::json!(5));

    // SUM over zero non-NULL values is NULL (not 0), matching SQL.
    let mut sum = agg(AggFn::Sum, Some(1));
    sum.apply(&vec![Tup2(null_id, 1)]);
    assert_eq!(sum.value(), serde_json::Value::Null);
}

/// MIN(id) over the filtered set restores the previous extreme on retraction (the multiset).
#[test]
fn aggregate_min_with_retraction() {
    let mut a = agg(AggFn::Min, Some(1)); // col 1 = id (sorted: active,id,name)
    a.apply(&vec![Tup2(active(5), 1), Tup2(active(3), 1), Tup2(active(8), 1)]);
    assert_eq!(a.value(), serde_json::json!(3));
    a.apply(&vec![Tup2(active(3), -1)]); // remove the current min → next-smallest surfaces
    assert_eq!(a.value(), serde_json::json!(5));
    let mut mx = agg(AggFn::Max, Some(1));
    mx.apply(&vec![Tup2(active(5), 1), Tup2(active(8), 1)]);
    assert_eq!(mx.value(), serde_json::json!(8));
    mx.apply(&vec![Tup2(active(8), -1)]);
    assert_eq!(mx.value(), serde_json::json!(5));
}

// --- membership kernel (shared by the subquery registry and circuit cohort serving) ---------

/// Refcount flip detection: a flip is a group whose refcount crosses zero — internal count
/// changes produce none.
#[test]
fn membership_fold_refcount_flips() {
    use crate::subquery::FlipDir;
    let mut groups: HashMap<Value, i64> = HashMap::new();
    // First contributor → Enter.
    let flips = membership::fold_refcount_flips(&mut groups, [(Value::Int(7), 1)]);
    assert_eq!(flips.len(), 1);
    assert_eq!(flips[0].value, Value::Int(7));
    assert_eq!(flips[0].dir, FlipDir::Enter);
    // Second contributor → refcount 2, no flip.
    assert!(membership::fold_refcount_flips(&mut groups, [(Value::Int(7), 1)]).is_empty());
    // One leaves → refcount 1, no flip.
    assert!(membership::fold_refcount_flips(&mut groups, [(Value::Int(7), -1)]).is_empty());
    // Last one leaves → Leave, and the group is dropped from the map.
    let flips = membership::fold_refcount_flips(&mut groups, [(Value::Int(7), -1)]);
    assert_eq!(flips.len(), 1);
    assert_eq!(flips[0].dir, FlipDir::Leave);
    assert!(groups.is_empty());
    // A batched retract+insert of different values in one delta flips both.
    membership::fold_refcount_flips(&mut groups, [(Value::Int(1), 1)]);
    let flips =
        membership::fold_refcount_flips(&mut groups, [(Value::Int(1), -1), (Value::Int(2), 1)]);
    assert_eq!(flips.len(), 2);
    assert_eq!((flips[0].value.clone(), flips[0].dir), (Value::Int(1), FlipDir::Leave));
    assert_eq!((flips[1].value.clone(), flips[1].dir), (Value::Int(2), FlipDir::Enter));
}

/// THE cross-implementation regression test for the two membership paths: for the same logical
/// sequence of inner-row changes, the circuit cohort's refcounted fold and the registry's
/// identity-reconciled `SubqueryNode` must report the SAME flips. If these ever diverge, the
/// same membership change would move rows on one serving tier and not the other.
#[test]
fn membership_flips_agree_between_refcount_and_contributor_set() {
    use crate::subquery::{Flip, SubqueryNode};
    // Scenario: rows (pk, projected value) — insert a→7, insert b→7, move a 7→8, delete b,
    // delete a. Expected flips: Enter 7, (none), Enter 8, Leave 7, Leave 8.
    // Registry path: reconcile by identity.
    let mut node = SubqueryNode::new(
        "sig".into(), "inner".into(), 0, 1, Arc::new(CompiledPredicate::MatchAll),
    );
    let mut reg_flips: Vec<Vec<Flip>> = Vec::new();
    reg_flips.push(node.reconcile_row("a", Some(Value::Int(7))));
    reg_flips.push(node.reconcile_row("b", Some(Value::Int(7))));
    reg_flips.push(node.reconcile_row("a", Some(Value::Int(8))));
    reg_flips.push(node.reconcile_row("b", None));
    reg_flips.push(node.reconcile_row("a", None));
    // Circuit path: the same changes as exactly-once weighted contributions.
    let mut groups: HashMap<Value, i64> = HashMap::new();
    let mut ref_flips: Vec<Vec<Flip>> = Vec::new();
    ref_flips.push(membership::fold_refcount_flips(&mut groups, [(Value::Int(7), 1)]));
    ref_flips.push(membership::fold_refcount_flips(&mut groups, [(Value::Int(7), 1)]));
    ref_flips
        .push(membership::fold_refcount_flips(&mut groups, [(Value::Int(7), -1), (Value::Int(8), 1)]));
    ref_flips.push(membership::fold_refcount_flips(&mut groups, [(Value::Int(7), -1)]));
    ref_flips.push(membership::fold_refcount_flips(&mut groups, [(Value::Int(8), -1)]));
    // Same flips at every step (order within a step normalized by value).
    let norm = |mut v: Vec<Flip>| {
        v.sort_by(|a, b| format!("{:?}", a.value).cmp(&format!("{:?}", b.value)));
        v
    };
    for (i, (r, c)) in reg_flips.into_iter().zip(ref_flips).enumerate() {
        assert_eq!(norm(r), norm(c), "flip divergence at step {i}");
    }
    assert!(groups.is_empty());
}

/// The latest-row-per-pk fold behind absolute membership evaluation: an update's `+1` row wins
/// over its `-1` retraction, a pure delete keeps the old row with `is_new = false`.
#[test]
fn membership_latest_rows_by_pk() {
    let ts = users(); // cols sorted: active, id, name — pk = id (index 1)
    let row = |id: i64, name: &str| Row(vec![Value::Bool(true), Value::Int(id), Value::Text(name.into())]);
    // Update of pk 1 (retract old + insert new) + delete of pk 2, one delta.
    let delta = vec![
        Tup2(row(1, "old"), -1),
        Tup2(row(1, "new"), 1),
        Tup2(row(2, "gone"), -1),
    ];
    let mut out = membership::latest_rows_by_pk(&ts, &delta);
    out.sort_by_key(|(r, _)| match r.0[1] { Value::Int(i) => i, _ => 0 });
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], (row(1, "new"), true));  // update → latest row, still exists
    assert_eq!(out[1], (row(2, "gone"), false)); // delete → old row, gone
}

// --- aggregate tier: shared envelope + conjunct-index pruning --------------------------------

/// Both aggregate tiers (in-engine fold and circuit-served counts) emit through ONE envelope
/// builder — same key, same `{value, n}` payload shape, same operation. This is the wire-format
/// contract a client materializes one row from.
#[test]
fn agg_envelope_shared_wire_format() {
    let ts = users();
    let mut a = agg(AggFn::Count, None);
    a.apply(&vec![Tup2(active(1), 1), Tup2(active(2), 1)]);
    let fold_env = a.envelope(&ts, Some("t1".into()), Some("0/1".into()));
    let circuit = CircuitAgg { stream_path: "shape/x".into(), constraints: vec![None], value: 2 };
    let circuit_env = circuit.envelope("users", Some("t1".into()), Some("0/1".into()));
    for env in [&fold_env, &circuit_env] {
        assert_eq!(env.key, "agg");
        assert_eq!(env.headers.operation, "upsert");
        let v = env.value.as_ref().unwrap();
        assert_eq!(v["value"], serde_json::json!(2));
        assert_eq!(v["n"], serde_json::json!(2));
    }
    assert_eq!(fold_env.value, circuit_env.value);
}

/// The aggregate conjunct index prunes exactly like the standalone one: an equality-conjunct
/// aggregate is a candidate only for deltas satisfying its leaf; match-all aggregates stay on
/// the scan list (always candidates).
#[test]
fn agg_index_candidates_prune() {
    let ts = users(); // cols sorted: active(0), id(1), name(2)
    let mut idx = StandaloneIndex::default();
    let eq_pred = CompiledPredicate::compile(
        &serde_json::from_value(serde_json::json!({"col":"name","op":"eq","value":"alice"}))
            .unwrap(),
        &ts,
    )
    .unwrap();
    idx.insert("agg-eq", &eq_pred);
    idx.insert("agg-all", &CompiledPredicate::MatchAll);
    let row = |name: &str| Row(vec![Value::Bool(true), Value::Int(1), Value::Text(name.into())]);
    // A bob-row delta: only the match-all aggregate is a candidate.
    let c: HashSet<String> = idx.candidates(&[Tup2(row("bob"), 1)]).into_iter().collect();
    assert!(c.contains("agg-all") && !c.contains("agg-eq"));
    // An alice-row delta: both are candidates.
    let c: HashSet<String> = idx.candidates(&[Tup2(row("alice"), 1)]).into_iter().collect();
    assert!(c.contains("agg-all") && c.contains("agg-eq"));
    // Removal cleans the index.
    idx.remove("agg-eq");
    let c: HashSet<String> = idx.candidates(&[Tup2(row("alice"), 1)]).into_iter().collect();
    assert!(!c.contains("agg-eq"));
}
