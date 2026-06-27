//! A circuit actor: one dbsp filter circuit per shape, confined to a dedicated OS thread
//! (`CircuitHandle` is `!Send`). The async side feeds input Z-set deltas over a channel and
//! awaits the filtered output deltas.

use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::{Result, anyhow};
use dbsp::utils::Tup2;
use dbsp::{IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit, ZSetHandle, ZWeight};
use tokio::sync::{mpsc, oneshot};

use crate::predicate::CompiledPredicate;
use crate::value::Row;

type Req = (Vec<Tup2<Row, ZWeight>>, oneshot::Sender<Vec<(Row, ZWeight)>>);

pub struct CircuitActor {
    tx: mpsc::UnboundedSender<Req>,
    _handle: JoinHandle<()>,
}

impl CircuitActor {
    /// Build a `WHERE`-filter circuit for `pred` on its own thread.
    pub fn spawn(pred: Arc<CompiledPredicate>) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<Req>();
        let handle = std::thread::Builder::new()
            .name("el-circuit".into())
            .spawn(move || run(pred, rx))?;
        Ok(Self { tx, _handle: handle })
    }

    /// Apply an input Z-set delta and return the filtered output delta (signed weights).
    pub async fn process(&self, delta: Vec<Tup2<Row, ZWeight>>) -> Result<Vec<(Row, ZWeight)>> {
        let (rtx, rrx) = oneshot::channel();
        self.tx.send((delta, rtx)).map_err(|_| anyhow!("circuit actor is gone"))?;
        rrx.await.map_err(|_| anyhow!("circuit actor dropped the reply"))
    }
}

fn run(pred: Arc<CompiledPredicate>, mut rx: mpsc::UnboundedReceiver<Req>) {
    let build = move |circuit: &mut RootCircuit| -> Result<(ZSetHandle<Row>, OutputHandle<OrdZSet<Row>>)> {
        let (stream, input) = circuit.add_input_zset::<Row>();
        let p = pred.clone();
        let filtered = stream.filter(move |row| p.eval(row));
        Ok((input, filtered.output()))
    };
    let (circuit, (input, output)) = match RootCircuit::build(build) {
        Ok(x) => x,
        Err(e) => {
            tracing::error!("failed to build circuit: {e:#}");
            return;
        }
    };
    // Drains requests until all senders drop (shape removed / engine shut down).
    while let Some((mut delta, reply)) = rx.blocking_recv() {
        input.append(&mut delta);
        match circuit.transaction() {
            Ok(()) => {
                let out: Vec<(Row, ZWeight)> =
                    output.consolidate().iter().map(|(r, (), w)| (r, w)).collect();
                let _ = reply.send(out);
            }
            Err(e) => {
                tracing::error!("circuit transaction failed: {e:#}");
                let _ = reply.send(Vec::new());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::{CompiledPredicate, LeafOp};
    use crate::value::Value;

    fn row(id: i64, active: bool) -> Row {
        // positional [active, id] doesn't matter here; predicate uses index 1 for "id".
        Row(vec![Value::Bool(active), Value::Int(id)])
    }

    #[tokio::test]
    async fn filter_actor_emits_signed_deltas() {
        // predicate: column 0 (active) == true
        let pred = Arc::new(CompiledPredicate::Cmp { col: 0, op: LeafOp::Eq, value: Value::Bool(true) });
        let actor = CircuitActor::spawn(pred).unwrap();

        // insert two rows; only the active one passes.
        let out = actor
            .process(vec![Tup2(row(1, true), 1), Tup2(row(2, false), 1)])
            .await
            .unwrap();
        assert_eq!(out, vec![(row(1, true), 1)]);

        // flip row 1 inactive: delta = -old(active) +new(inactive). old passes filter (-1),
        // new doesn't (dropped) => output is a single -1 leaving the shape.
        let out = actor
            .process(vec![Tup2(row(1, true), -1), Tup2(row(1, false), 1)])
            .await
            .unwrap();
        assert_eq!(out, vec![(row(1, true), -1)]);
    }
}
