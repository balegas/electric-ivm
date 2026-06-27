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
}

enum TailerCmd {
    AddShape { shape_id: String, stream_path: String, pred: Arc<CompiledPredicate> },
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

        let id = format!("s{}", st.next_shape_id);
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
            .send(TailerCmd::AddShape { shape_id: id.clone(), stream_path: stream_path.clone(), pred })
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
}

struct ShapeActor {
    actor: CircuitActor,
    stream_path: String,
}

fn spawn_tailer(ds: DsClient, ts: TableSchema) -> TailerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(tailer_loop(ds, ts, cmd_rx));
    TailerHandle { cmd_tx }
}

async fn tailer_loop(ds: DsClient, ts: TableSchema, mut cmd_rx: mpsc::UnboundedReceiver<TailerCmd>) {
    let table_path = format!("table/{}", ts.name);
    let mut offset = "-1".to_string();
    let mut table_state: HashMap<Value, Row> = HashMap::new();
    let mut shapes: HashMap<String, ShapeActor> = HashMap::new();

    loop {
        let off = offset.clone();
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(TailerCmd::AddShape { shape_id, stream_path, pred }) => {
                    match add_shape(&ds, &ts, &table_state, stream_path, pred).await {
                        Ok(actor) => { shapes.insert(shape_id, actor); }
                        Err(e) => tracing::error!("add_shape({shape_id}) failed: {e:#}"),
                    }
                }
                Some(TailerCmd::RemoveShape { shape_id }) => { shapes.remove(&shape_id); }
                None => break,
            },
            res = ds.read(&table_path, &off, true) => match res {
                Ok(rr) => {
                    if let Some(n) = rr.next_offset { offset = n; }
                    for env in rr.envelopes {
                        if let Err(e) = process_envelope(&ds, &ts, &mut table_state, &shapes, env).await {
                            tracing::error!("process_envelope failed: {e:#}");
                        }
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
    env: Envelope,
) -> Result<()> {
    let (delta, txid) = apply_envelope(ts, table_state, &env)?;
    if delta.is_empty() {
        return Ok(());
    }
    for shape in shapes.values() {
        let out = shape.actor.process(delta.clone()).await?;
        if out.is_empty() {
            continue;
        }
        let envs = translate_output(ts, out, txid.clone());
        ds.append(&shape.stream_path, &envs).await?;
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
    for pk in &neg {
        if pos.contains_key(pk) {
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
