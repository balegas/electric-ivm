//! The shared envelope codec: change-log envelope -> Z-set delta, and output delta ->
//! per-pk State-Protocol envelopes. Used by the sequencer and the subquery registry.

use super::*;

/// Turn a table change event into the resulting input Z-set delta, plus the originating txid and
/// commit LSN. The delta is computed entirely from the envelope's `value` (new row) and `old` (prior
/// row, carried by replication under `REPLICA IDENTITY FULL`) — no in-memory `table_state`.
pub(crate) fn apply_envelope(
    ts: &TableSchema,
    env: &Envelope,
) -> Result<(Vec<Tup2<Row, ZWeight>>, Option<String>, Option<String>)> {
    let txid = env.headers.txid.clone();
    let lsn = env.headers.lsn.clone();
    let to_row = |v: &serde_json::Value| -> Result<Row> {
        let obj = v.as_object().ok_or_else(|| anyhow::anyhow!("envelope row is not an object"))?;
        ts.row_from_json(obj)
    };
    let mut delta: Vec<Tup2<Row, ZWeight>> = Vec::new();
    match env.headers.operation.as_str() {
        "insert" => {
            let new = to_row(env.value.as_ref().context("insert envelope missing value")?)?;
            delta.push(Tup2(new, 1));
        }
        "update" | "upsert" => {
            let new = to_row(env.value.as_ref().context("update envelope missing value")?)?;
            match env.old.as_ref() {
                Some(old) => {
                    let old = to_row(old)?;
                    if old != new {
                        delta.push(Tup2(old, -1));
                        delta.push(Tup2(new, 1));
                    }
                }
                // No prior row available -> treat as an insert of the new row.
                None => delta.push(Tup2(new, 1)),
            }
        }
        "delete" => {
            // Replication carries the full old row (REPLICA IDENTITY FULL); retract it.
            if let Some(old) = env.old.as_ref() {
                delta.push(Tup2(to_row(old)?, -1));
            }
        }
        other => bail!("unknown operation '{other}'"),
    }
    Ok((delta, txid, lsn))
}

/// Translate a shape circuit's output Z-set delta into State-Protocol envelopes. Grouped by pk:
/// any positive-weight row -> `upsert` (enter/update); otherwise `delete` (leave).
pub(crate) fn translate_output(
    ts: &TableSchema,
    out: Vec<(Row, ZWeight)>,
    txid: Option<String>,
    lsn: Option<String>,
    out_cols: Option<&[usize]>,
) -> Vec<Envelope> {
    let mut pos: HashMap<String, Row> = HashMap::new();
    let mut neg: HashSet<String> = HashSet::new();
    for (row, w) in out {
        let pk = match ts.key_string(&row) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::warn!("translate_output: dropping row with unextractable pk on table {}: {e:#}", ts.name);
                continue;
            }
        };
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
            value: Some(ts.row_to_json_cols(row, out_cols)),
            old: None,
            headers: EnvelopeHeaders { operation: "upsert".into(), txid: txid.clone(), offset: None, lsn: lsn.clone(), seq: None },
        });
    }
    // TEST-ONLY: the `drop_deletes` fault suppresses "leave" envelopes so rows that exit a shape
    // linger in the client. No-op unless ELECTRIC_IVM_FAULT=drop_deletes (see `fault`).
    let drop_deletes = matches!(crate::fault::active(), crate::fault::Fault::DropDeletes);
    for pk in &neg {
        if pos.contains_key(pk) || drop_deletes {
            continue;
        }
        envs.push(Envelope {
            type_: ts.name.clone(),
            key: pk.clone(),
            value: None,
            old: None,
            headers: EnvelopeHeaders { operation: "delete".into(), txid: txid.clone(), offset: None, lsn: lsn.clone(), seq: None },
        });
    }
    envs
}

/// Key-only `delete` envelopes for pks the per-feed relation retracted. The feed relation's
/// retraction IS the delete decision (structural spurious-delete gating), so this needs no
/// row body — only the pk. Honors the TEST-ONLY `drop_deletes` fault exactly like
/// [`translate_output`].
pub(crate) fn delete_envelopes(ts: &TableSchema, pks: Vec<String>, txid: Option<String>) -> Vec<Envelope> {
    if matches!(crate::fault::active(), crate::fault::Fault::DropDeletes) {
        return Vec::new();
    }
    pks.into_iter()
        .map(|pk| Envelope {
            type_: ts.name.clone(),
            key: pk,
            value: None,
            old: None,
            headers: EnvelopeHeaders { operation: "delete".into(), txid: txid.clone(), offset: None, lsn: None, seq: None },
        })
        .collect()
}

/// The aggregate wire envelope — ONE `"agg"`-keyed row `{ value, n }`, upserted when the value
/// changes. Shared by the in-engine fold ([`super::executors::AggShape`]) and circuit-served
/// counts ([`super::executors::CircuitAgg`]) so the two aggregate tiers cannot drift apart on
/// the wire format.
pub(crate) fn agg_envelope(
    table: &str,
    value: serde_json::Value,
    n: i64,
    txid: Option<String>,
    lsn: Option<String>,
) -> Envelope {
    Envelope {
        type_: table.to_string(),
        key: "agg".into(),
        value: Some(serde_json::json!({ "value": value, "n": n })),
        old: None,
        headers: EnvelopeHeaders { operation: "upsert".into(), txid, offset: None, lsn, seq: None },
    }
}
