//! A "family" circuit: one dbsp circuit shared by every shape whose predicate is the same equality
//! template modulo constants. Instead of one `filter` per shape, the table is indexed once by the
//! template's key columns and equi-joined with a `Params{(key_tuple, shape_id)}` collection; the
//! join emits `(shape_id, row)` for each match, which the tailer demultiplexes per shape.
//!
//! Adding a shape is an insert into `Params` (the incremental join emits its backfill); dropping a
//! shape is a delete. See `docs/superpowers/specs/2026-06-27-shape-pipeline-sharing-design.md`.

use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::{Result, anyhow};
use dbsp::circuit::CircuitConfig;
use dbsp::utils::Tup2;
use dbsp::{IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit, Runtime, ZSetHandle, ZWeight};
use tokio::sync::{mpsc, oneshot};

use crate::value::{Row, Value};

/// Params element: `(key_tuple, shape_id)`. `shape_id` is the numeric id; the tailer maps it back
/// to a stream path for demultiplexing.
type ParamElem = Tup2<Row, u64>;

type Req = (
    Vec<Tup2<Row, ZWeight>>,       // data delta (table changes)
    Vec<Tup2<ParamElem, ZWeight>>, // params delta (shape add/remove)
    oneshot::Sender<Vec<(u64, Row, ZWeight)>>,
);

pub struct FamilyActor {
    tx: mpsc::UnboundedSender<Req>,
    _handle: JoinHandle<()>,
}

impl FamilyActor {
    /// Build the shared circuit for a template whose key is the given (sorted) column indices.
    pub fn spawn(key_cols: Arc<Vec<usize>>) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<Req>();
        let handle = std::thread::Builder::new()
            .name("el-family".into())
            .spawn(move || run(key_cols, rx))?;
        Ok(Self { tx, _handle: handle })
    }

    /// Apply a data delta and/or a params delta in one transaction; return the joined
    /// `(shape_id, row, weight)` output delta.
    pub async fn step(
        &self,
        data: Vec<Tup2<Row, ZWeight>>,
        params: Vec<Tup2<ParamElem, ZWeight>>,
    ) -> Result<Vec<(u64, Row, ZWeight)>> {
        let (rtx, rrx) = oneshot::channel();
        self.tx.send((data, params, rtx)).map_err(|_| anyhow!("family actor is gone"))?;
        rrx.await.map_err(|_| anyhow!("family actor dropped the reply"))
    }
}

/// The key tuple for a row given the template's key columns (positional projection).
pub fn key_of(row: &Row, cols: &[usize]) -> Row {
    Row(cols.iter().map(|&i| row.0.get(i).cloned().unwrap_or(Value::Null)).collect())
}

fn run(key_cols: Arc<Vec<usize>>, mut rx: mpsc::UnboundedReceiver<Req>) {
    type Built = (ZSetHandle<Row>, ZSetHandle<ParamElem>, OutputHandle<OrdZSet<Tup2<u64, Row>>>);
    let build = move |circuit: &mut RootCircuit| -> Result<Built> {
        let (data_s, data_in) = circuit.add_input_zset::<Row>();
        let (params_s, params_in) = circuit.add_input_zset::<ParamElem>();
        let kc = key_cols.clone();
        // data indexed by key tuple; params indexed by the same key tuple.
        let data_idx = data_s.map_index(move |row| (key_of(row, &kc), row.clone()));
        let params_idx = params_s.map_index(|Tup2(key, shape)| (key.clone(), *shape));
        // equi-join: (shape_id, row) for every (row, shape) sharing a key.
        let joined = data_idx.join(&params_idx, |_key, row, shape| Tup2(*shape, row.clone()));
        Ok((data_in, params_in, joined.output()))
    };
    // The join uses traces/spines, which require a DBSP runtime (a plain `RootCircuit::build` has
    // none). One worker keeps the per-family circuit single-threaded and deterministic.
    let (mut circuit, (data_in, params_in, output)) =
        match Runtime::init_circuit(CircuitConfig::from(1usize), build) {
            Ok(x) => x,
            Err(e) => {
                tracing::error!("failed to build family circuit: {e:#}");
                return;
            }
        };
    while let Some((mut data, mut params, reply)) = rx.blocking_recv() {
        data_in.append(&mut data);
        params_in.append(&mut params);
        match circuit.transaction() {
            Ok(()) => {
                let out: Vec<(u64, Row, ZWeight)> = output
                    .consolidate()
                    .iter()
                    .map(|(Tup2(shape, row), (), w)| (shape, row, w))
                    .collect();
                let _ = reply.send(out);
            }
            Err(e) => {
                tracing::error!("family circuit transaction failed: {e:#}");
                let _ = reply.send(Vec::new());
            }
        }
    }
    let _ = circuit.kill();
}

#[cfg(test)]
mod tests {
    use super::*;

    // rows are [key_col, id]; the family keys on column 0.
    fn row(k: i64, id: i64) -> Row {
        Row(vec![Value::Int(k), Value::Int(id)])
    }
    fn param(k: i64, shape: u64, w: ZWeight) -> Tup2<ParamElem, ZWeight> {
        Tup2(Tup2(Row(vec![Value::Int(k)]), shape), w)
    }
    fn sorted(mut v: Vec<(u64, Row, ZWeight)>) -> Vec<(u64, Row, ZWeight)> {
        v.sort_by(|a, b| (a.0, &a.1.0).cmp(&(b.0, &b.1.0)));
        v
    }

    #[tokio::test]
    async fn equi_join_routes_rows_to_shapes_by_key() {
        let fam = FamilyActor::spawn(Arc::new(vec![0])).unwrap();

        // Prime data (keys 1 and 2) and add shape 100 on key=1 in one step -> backfill for 100 only.
        let out = fam
            .step(vec![Tup2(row(1, 10), 1), Tup2(row(2, 20), 1)], vec![param(1, 100, 1)])
            .await
            .unwrap();
        assert_eq!(out, vec![(100, row(1, 10), 1)]);

        // Add shape 200 on key=2 -> backfill against the already-accumulated data trace.
        let out = fam.step(vec![], vec![param(2, 200, 1)]).await.unwrap();
        assert_eq!(out, vec![(200, row(2, 20), 1)]);

        // A new row on key=1 enters shape 100 (only); a new row on key=2 enters 200.
        let out = sorted(fam.step(vec![Tup2(row(1, 11), 1), Tup2(row(2, 21), 1)], vec![]).await.unwrap());
        assert_eq!(out, sorted(vec![(100, row(1, 11), 1), (200, row(2, 21), 1)]));

        // Drop shape 100 (param -1) -> its rows leave (negative weights), shape 200 untouched.
        let out = sorted(fam.step(vec![], vec![param(1, 100, -1)]).await.unwrap());
        assert_eq!(out, sorted(vec![(100, row(1, 10), -1), (100, row(1, 11), -1)]));

        // After the drop, a change on key=1 no longer produces output for shape 100.
        let out = fam.step(vec![Tup2(row(1, 12), 1)], vec![]).await.unwrap();
        assert_eq!(out, vec![]);
    }
}
