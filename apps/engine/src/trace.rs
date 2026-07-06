//! Per-envelope pipeline trace: a best-effort broadcast of the route each replicated change took
//! through the maintained pipeline (which family routers / filters / subquery nodes it hit, with
//! what outcome, and which shape streams got appends). Consumed by `GET /trace` (SSE) for
//! visualization/debugging. Delivery is lossy by design: a bounded broadcast channel, no
//! backpressure into the hot path, and zero cost when nobody is subscribed
//! (`receiver_count() == 0` short-circuits before any serialization).
//!
//! Node ids use the same namespace the pipeline visualizer's logical view derives from `/graph`
//! (`apps/pipeline-viz/src/build-graph.ts`): `table:<t>`, `filter:<shape-id>`,
//! `family:<t>:<col,col>`, `node:<subquery-sig>`, `shape:<shape-id>` — so a UI can animate trace
//! events onto the graph without translation.

use serde::Serialize;

/// Capacity of the trace broadcast channel. Slow subscribers lag and drop events rather than
/// slowing envelope processing.
pub const CHANNEL_CAP: usize = 1024;

/// How many weighted delta rows a single event carries at most (a UI animates a few dots, not a
/// bulk backfill).
pub const DELTA_CAP: usize = 8;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txid: Option<String>,
    pub table: String,
    /// Weighted rows of this envelope's delta (capped at [`DELTA_CAP`]).
    pub delta: Vec<TraceDelta>,
    /// Pipeline nodes visited, in fan-out order, with the outcome at each.
    pub hops: Vec<TraceHop>,
    /// Shape ids whose streams got appends from this envelope.
    pub shapes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceDelta {
    pub row: serde_json::Value,
    pub w: i64,
}

/// Graph-lifecycle event, broadcast on the same channel as [`TraceEvent`]: creating or dropping a
/// shape changes the pipeline's *structure* (new filters/routers/nodes and the paths between
/// them), which a UI highlights differently from data flow. Distinguished on the wire by the
/// `type` field, which data events don't carry.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum GraphLifecycle {
    #[serde(rename_all = "camelCase")]
    ShapeAdded { shape: String, table: String },
    #[serde(rename_all = "camelCase")]
    ShapeDropped { shape: String },
}

/// Outcome of one node visit. `passed` = the delta (or part of it) continued downstream;
/// `dropped` = it terminated here (filter mismatch, no routing key, snapshot-gate skip, no
/// inner-set change); `routed` = a family router dispatched it (with the key values); `folded` =
/// an aggregation absorbed it into its running scalar.
#[derive(Debug, Clone, Serialize)]
pub struct TraceHop {
    pub node: String,
    pub outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<serde_json::Value>,
}

impl TraceHop {
    pub fn new(node: String, outcome: &'static str) -> Self {
        TraceHop { node, outcome, key: None }
    }
    pub fn routed(node: String, key: serde_json::Value) -> Self {
        TraceHop { node, outcome: "routed", key: Some(key) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_event_serializes_camel_case() {
        let ev = TraceEvent {
            lsn: Some("0/1A2B3C".into()),
            txid: None,
            table: "orders".into(),
            delta: vec![TraceDelta { row: serde_json::json!({"id": 1}), w: -1 }],
            hops: vec![
                TraceHop::routed("family:orders:status,workspace_id".into(), serde_json::json!(["cooking", "w1"])),
                TraceHop::new("filter:s7".into(), "dropped"),
            ],
            shapes: vec!["s3".into()],
        };
        let v: serde_json::Value = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["lsn"], "0/1A2B3C");
        assert!(v.get("txid").is_none(), "None fields are skipped");
        assert_eq!(v["table"], "orders");
        assert_eq!(v["delta"][0]["w"], -1);
        assert_eq!(v["hops"][0]["outcome"], "routed");
        assert_eq!(v["hops"][0]["key"][0], "cooking");
        assert_eq!(v["hops"][1]["outcome"], "dropped");
        assert!(v["hops"][1].get("key").is_none());
        assert_eq!(v["shapes"][0], "s3");
    }
}
