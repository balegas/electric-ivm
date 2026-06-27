//! Engine orchestration: schema/shape registries and one tailer task per table. A tailer owns
//! the table's authoritative `pk -> Row` state, fans each change out to every shape's circuit
//! actor, and appends the filtered deltas (as State-Protocol envelopes) to the shape streams.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Result, bail};
use dbsp::ZWeight;
use dbsp::utils::Tup2;
use tokio::sync::{Mutex, mpsc};

use crate::circuit::CircuitActor;
use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::family::FamilyActor;
use crate::predicate::{CompiledPredicate, PredicateJson};
use crate::schema::{Schema, TableSchema, compile_schema};
use crate::value::{Row, Value};

#[derive(Clone)]
pub struct Engine {
    ds: DsClient,
    state: Arc<Mutex<EngineState>>,
}

struct EngineState {
    tables: HashMap<String, TableSchema>,
    tailers: HashMap<String, TailerHandle>,
    shapes: HashMap<String, ShapeRecord>,
    next_shape_id: u64,
}

#[derive(Clone, Debug)]
pub struct ShapeRecord {
    pub id: String,
    pub table: String,
    pub stream_path: String,
}

struct TailerHandle {
    cmd_tx: mpsc::UnboundedSender<TailerCmd>,
    /// Offset up to which all table-stream envelopes have been processed AND fanned to every
    /// shape. Published after a batch is fully processed; a harness can poll this to know the
    /// engine has caught up to the stream tail (a sound convergence barrier).
    processed: Arc<std::sync::Mutex<String>>,
}

enum TailerCmd {
    AddShape { shape_id: String, num_id: u64, stream_path: String, pred: Arc<CompiledPredicate> },
    RemoveShape { shape_id: String },
}

impl Engine {
    pub fn new(ds: DsClient) -> Self {
        Engine {
            ds,
            state: Arc::new(Mutex::new(EngineState {
                tables: HashMap::new(),
                tailers: HashMap::new(),
                shapes: HashMap::new(),
                next_shape_id: 1,
            })),
        }
    }

    pub fn stream_url(&self, path: &str) -> String {
        self.ds.stream_url(path)
    }

    pub async fn define_schema(&self, schema: &Schema) -> Result<()> {
        let compiled = compile_schema(schema)?;
        for name in compiled.keys() {
            self.ds.ensure_stream(&format!("table/{name}")).await?;
        }
        self.state.lock().await.tables = compiled;
        Ok(())
    }

    pub async fn create_shape(&self, table: &str, where_: Option<PredicateJson>) -> Result<ShapeRecord> {
        let mut st = self.state.lock().await;
        let ts = match st.tables.get(table) {
            Some(ts) => ts.clone(),
            None => bail!("unknown table '{table}'"),
        };
        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        self.ds.ensure_stream(&stream_path).await?;

        if !st.tailers.contains_key(table) {
            let handle = spawn_tailer(self.ds.clone(), ts.clone());
            st.tailers.insert(table.to_string(), handle);
        }
        let tailer = st.tailers.get(table).expect("tailer just inserted");
        tailer
            .cmd_tx
            .send(TailerCmd::AddShape { shape_id: id.clone(), num_id, stream_path: stream_path.clone(), pred })
            .map_err(|_| anyhow::anyhow!("tailer for '{table}' is gone"))?;

        let rec = ShapeRecord { id: id.clone(), table: table.to_string(), stream_path };
        st.shapes.insert(id, rec.clone());
        Ok(rec)
    }

    pub async fn drop_shape(&self, id: &str) -> Result<()> {
        let mut st = self.state.lock().await;
        if let Some(rec) = st.shapes.remove(id) {
            if let Some(t) = st.tailers.get(&rec.table) {
                let _ = t.cmd_tx.send(TailerCmd::RemoveShape { shape_id: id.to_string() });
            }
        }
        Ok(())
    }

    pub async fn get_shape(&self, id: &str) -> Option<ShapeRecord> {
        self.state.lock().await.shapes.get(id).cloned()
    }

    /// The offset up to which the table's tailer has processed, or `None` if no tailer exists
    /// (no shape registered on the table yet).
    pub async fn table_offset(&self, table: &str) -> Option<String> {
        let st = self.state.lock().await;
        st.tailers.get(table).map(|t| t.processed.lock().unwrap().clone())
    }
}

struct ShapeActor {
    actor: CircuitActor,
    stream_path: String,
}

/// A shared circuit for all shapes whose predicate is the same equality template (see
/// `family::FamilyActor`). `shapes` maps each member's numeric id to its output stream path.
struct Family {
    actor: FamilyActor,
    shapes: HashMap<u64, String>,
}

fn spawn_tailer(ds: DsClient, ts: TableSchema) -> TailerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let processed = Arc::new(std::sync::Mutex::new("-1".to_string()));
    tokio::spawn(tailer_loop(ds, ts, cmd_rx, processed.clone()));
    TailerHandle { cmd_tx, processed }
}

async fn tailer_loop(
    ds: DsClient,
    ts: TableSchema,
    mut cmd_rx: mpsc::UnboundedReceiver<TailerCmd>,
    processed: Arc<std::sync::Mutex<String>>,
) {
    let table_path = format!("table/{}", ts.name);
    let mut offset = "-1".to_string();
    let mut table_state: HashMap<Value, Row> = HashMap::new();
    // Standalone per-shape filter circuits (non-equality predicates), keyed by shape id.
    let mut shapes: HashMap<String, ShapeActor> = HashMap::new();
    // Shared family circuits, keyed by the equality template's (sorted) column indices.
    let mut families: HashMap<Vec<usize>, Family> = HashMap::new();
    // Reverse lookup for removal: shape id -> (template key cols, numeric id, key tuple).
    let mut family_of: HashMap<String, (Vec<usize>, u64, Row)> = HashMap::new();

    loop {
        let off = offset.clone();
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(TailerCmd::AddShape { shape_id, num_id, stream_path, pred }) => {
                    if let Err(e) = add_shape_routed(
                        &ds, &ts, &table_state, &mut shapes, &mut families, &mut family_of,
                        shape_id, num_id, stream_path, pred,
                    ).await {
                        tracing::error!("add_shape failed: {e:#}");
                    }
                }
                Some(TailerCmd::RemoveShape { shape_id }) => {
                    if shapes.remove(&shape_id).is_none()
                        && let Some((key_cols, num_id, key_tuple)) = family_of.remove(&shape_id)
                        && let Some(fam) = families.get_mut(&key_cols)
                    {
                        // Drop the shape's param so future changes skip it; ignore the removal delta
                        // (the shape stream is being torn down).
                        let _ = fam.actor.step(vec![], vec![Tup2(Tup2(key_tuple, num_id), -1)]).await;
                        fam.shapes.remove(&num_id);
                        if fam.shapes.is_empty() {
                            families.remove(&key_cols); // discard the now-unused family + its trace
                        }
                    }
                }
                None => break,
            },
            res = ds.read(&table_path, &off, true) => match res {
                Ok(rr) => {
                    let next = rr.next_offset.clone();
                    if let Some(n) = rr.next_offset { offset = n; }
                    for env in rr.envelopes {
                        if let Err(e) = process_envelope(&ds, &ts, &mut table_state, &shapes, &families, env).await {
                            tracing::error!("process_envelope failed: {e:#}");
                        }
                    }
                    // Publish the processed offset only after the whole batch is fanned out.
                    if let Some(n) = next {
                        *processed.lock().unwrap() = n;
                    }
                }
                Err(e) => {
                    tracing::warn!("tailer read error on {table_path}: {e:#}; backing off");
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            },
        }
    }
}

/// Route a new shape to a shared family circuit (pure-equality predicate) or a standalone filter
/// circuit (everything else). For a family, adding the shape is a `Params` insert; its backfill is
/// the join of that param against the family's current data trace.
#[allow(clippy::too_many_arguments)]
async fn add_shape_routed(
    ds: &DsClient,
    ts: &TableSchema,
    table_state: &HashMap<Value, Row>,
    shapes: &mut HashMap<String, ShapeActor>,
    families: &mut HashMap<Vec<usize>, Family>,
    family_of: &mut HashMap<String, (Vec<usize>, u64, Row)>,
    shape_id: String,
    num_id: u64,
    stream_path: String,
    pred: Arc<CompiledPredicate>,
) -> Result<()> {
    match pred.equality_template() {
        Some(pairs) => {
            let key_cols: Vec<usize> = pairs.iter().map(|(c, _)| *c).collect();
            let key_tuple = Row(pairs.into_iter().map(|(_, v)| v).collect());
            let param = Tup2(Tup2(key_tuple.clone(), num_id), 1);

            if let Some(fam) = families.get_mut(&key_cols) {
                // Existing family: insert the param; the incremental join backfills from the trace.
                let out = fam.actor.step(vec![], vec![param]).await?;
                fam.shapes.insert(num_id, stream_path);
                emit_family_output(ds, ts, fam, out, None).await?;
            } else {
                // New family: prime the data trace with the current table state and add the param in
                // one step; the step output is this shape's backfill.
                let actor = FamilyActor::spawn(Arc::new(key_cols.clone()))?;
                let data: Vec<Tup2<Row, ZWeight>> =
                    table_state.values().map(|r| Tup2(r.clone(), 1)).collect();
                let out = actor.step(data, vec![param]).await?;
                let mut fam = Family { actor, shapes: HashMap::new() };
                fam.shapes.insert(num_id, stream_path);
                emit_family_output(ds, ts, &fam, out, None).await?;
                families.insert(key_cols.clone(), fam);
            }
            family_of.insert(shape_id, (key_cols, num_id, key_tuple));
        }
        None => {
            let actor = add_shape(ds, ts, table_state, stream_path, pred).await?;
            shapes.insert(shape_id, actor);
        }
    }
    Ok(())
}

/// Demultiplex a family circuit's `(shape_id, row, weight)` output by shape and append each shape's
/// rows (translated to envelopes) to its stream.
async fn emit_family_output(
    ds: &DsClient,
    ts: &TableSchema,
    fam: &Family,
    out: Vec<(u64, Row, ZWeight)>,
    txid: Option<String>,
) -> Result<()> {
    let mut by_shape: HashMap<u64, Vec<(Row, ZWeight)>> = HashMap::new();
    for (sid, row, w) in out {
        by_shape.entry(sid).or_default().push((row, w));
    }
    for (sid, rows) in by_shape {
        if let Some(stream_path) = fam.shapes.get(&sid) {
            let envs = translate_output(ts, rows, txid.clone());
            if !envs.is_empty() {
                ds.append(stream_path, &envs).await?;
            }
        }
    }
    Ok(())
}

async fn add_shape(
    ds: &DsClient,
    ts: &TableSchema,
    table_state: &HashMap<Value, Row>,
    stream_path: String,
    pred: Arc<CompiledPredicate>,
) -> Result<ShapeActor> {
    let actor = CircuitActor::spawn(pred)?;
    // Backfill: feed the table's current rows as inserts so the new shape reflects existing data.
    if !table_state.is_empty() {
        let delta: Vec<Tup2<Row, ZWeight>> =
            table_state.values().map(|r| Tup2(r.clone(), 1)).collect();
        let out = actor.process(delta).await?;
        let envs = translate_output(ts, out, None);
        ds.append(&stream_path, &envs).await?;
    }
    Ok(ShapeActor { actor, stream_path })
}

async fn process_envelope(
    ds: &DsClient,
    ts: &TableSchema,
    table_state: &mut HashMap<Value, Row>,
    shapes: &HashMap<String, ShapeActor>,
    families: &HashMap<Vec<usize>, Family>,
    env: Envelope,
) -> Result<()> {
    let (delta, txid) = apply_envelope(ts, table_state, &env)?;
    if delta.is_empty() {
        return Ok(());
    }
    // Standalone per-shape circuits: each filters the delta independently.
    for shape in shapes.values() {
        let out = shape.actor.process(delta.clone()).await?;
        if out.is_empty() {
            continue;
        }
        let envs = translate_output(ts, out, txid.clone());
        ds.append(&shape.stream_path, &envs).await?;
    }
    // Shared family circuits: one join per template fans the delta to all its shapes.
    for fam in families.values() {
        let out = fam.actor.step(delta.clone(), vec![]).await?;
        if out.is_empty() {
            continue;
        }
        emit_family_output(ds, ts, fam, out, txid.clone()).await?;
    }
    Ok(())
}

/// Apply a table change event to `table_state` and return the resulting input Z-set delta
/// (with weights) plus the originating txid (propagated to shape envelopes).
pub(crate) fn apply_envelope(
    ts: &TableSchema,
    table_state: &mut HashMap<Value, Row>,
    env: &Envelope,
) -> Result<(Vec<Tup2<Row, ZWeight>>, Option<String>)> {
    let txid = env.headers.txid.clone();
    let mut delta: Vec<Tup2<Row, ZWeight>> = Vec::new();
    match env.headers.operation.as_str() {
        "insert" | "update" | "upsert" => {
            let value = env
                .value
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("{} envelope missing value", env.headers.operation))?;
            let obj = value
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("envelope value is not an object"))?;
            let row = ts.row_from_json(obj)?;
            let pk = ts.pk_of(&row)?.clone();
            match table_state.get(&pk) {
                Some(old) if old == &row => {}
                Some(old) => {
                    delta.push(Tup2(old.clone(), -1));
                    delta.push(Tup2(row.clone(), 1));
                }
                None => delta.push(Tup2(row.clone(), 1)),
            }
            table_state.insert(pk, row);
        }
        "delete" => {
            let pk = Value::from_key_string(&env.key, ts.pk_type)?;
            if let Some(old) = table_state.remove(&pk) {
                delta.push(Tup2(old, -1));
            }
        }
        other => bail!("unknown operation '{other}'"),
    }
    Ok((delta, txid))
}

/// Translate a shape circuit's output Z-set delta into State-Protocol envelopes. Grouped by pk:
/// any positive-weight row -> `upsert` (enter/update); otherwise `delete` (leave).
pub(crate) fn translate_output(ts: &TableSchema, out: Vec<(Row, ZWeight)>, txid: Option<String>) -> Vec<Envelope> {
    let mut pos: HashMap<String, Row> = HashMap::new();
    let mut neg: HashSet<String> = HashSet::new();
    for (row, w) in out {
        let Ok(pk) = ts.pk_of(&row).map(Value::to_key_string) else { continue };
        if w > 0 {
            pos.insert(pk, row);
        } else if w < 0 {
            neg.insert(pk);
        }
    }
    let mut envs = Vec::with_capacity(pos.len() + neg.len());
    for (pk, row) in &pos {
        envs.push(Envelope {
            type_: ts.name.clone(),
            key: pk.clone(),
            value: Some(ts.row_to_json(row)),
            headers: EnvelopeHeaders { operation: "upsert".into(), txid: txid.clone(), offset: None },
        });
    }
    // TEST-ONLY: the `drop_deletes` fault suppresses "leave" envelopes so rows that exit a shape
    // linger in the client. No-op unless ELECTRIC_LITE_FAULT=drop_deletes (see `fault`).
    let drop_deletes = matches!(crate::fault::active(), crate::fault::Fault::DropDeletes);
    for pk in &neg {
        if pos.contains_key(pk) || drop_deletes {
            continue;
        }
        envs.push(Envelope {
            type_: ts.name.clone(),
            key: pk.clone(),
            value: None,
            headers: EnvelopeHeaders { operation: "delete".into(), txid: txid.clone(), offset: None },
        });
    }
    envs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TableDef, TableSchema};

    fn users() -> TableSchema {
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "active": {"type":"bool"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def("users", &def).unwrap()
    }

    fn env(op: &str, key: &str, value: Option<serde_json::Value>) -> Envelope {
        Envelope {
            type_: "users".into(),
            key: key.into(),
            value,
            headers: EnvelopeHeaders { operation: op.into(), txid: None, offset: None },
        }
    }

    /// End-to-end (sans HTTP): change event -> input delta -> circuit -> output envelopes,
    /// exercising enter / update / leave for a `WHERE active = true` shape.
    #[tokio::test]
    async fn change_to_shape_envelope_enter_update_leave() {
        let ts = users();
        let pred = Arc::new(CompiledPredicate::compile_opt(
            Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap()),
            &ts,
        ).unwrap());
        let actor = CircuitActor::spawn(pred).unwrap();
        let mut table_state = HashMap::new();

        // enter: insert an active row -> upsert envelope
        let (delta, _) = apply_envelope(&ts, &mut table_state, &env("insert", "1", Some(serde_json::json!({"id":1,"name":"a","active":true})))).unwrap();
        let envs = translate_output(&ts, actor.process(delta).await.unwrap(), None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "upsert");
        assert_eq!(envs[0].key, "1");

        // update within shape (name change, still active) -> upsert with new value
        let (delta, _) = apply_envelope(&ts, &mut table_state, &env("update", "1", Some(serde_json::json!({"id":1,"name":"a2","active":true})))).unwrap();
        let envs = translate_output(&ts, actor.process(delta).await.unwrap(), None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "upsert");
        assert_eq!(envs[0].value.as_ref().unwrap()["name"], "a2");

        // leave: becomes inactive -> delete envelope
        let (delta, _) = apply_envelope(&ts, &mut table_state, &env("update", "1", Some(serde_json::json!({"id":1,"name":"a2","active":false})))).unwrap();
        let envs = translate_output(&ts, actor.process(delta).await.unwrap(), None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "delete");
        assert_eq!(envs[0].key, "1");

        // a non-matching insert produces no shape envelope
        let (delta, _) = apply_envelope(&ts, &mut table_state, &env("insert", "2", Some(serde_json::json!({"id":2,"name":"b","active":false})))).unwrap();
        let envs = translate_output(&ts, actor.process(delta).await.unwrap(), None);
        assert_eq!(envs.len(), 0);
    }
}
