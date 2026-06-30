//! Electric-protocol adapter: serves `GET /v1/shape` so Electric's official `Electric.Client` (and its
//! oracle test harness) can read shapes from our engine. We translate the engine's materialized-shape
//! durable stream into Electric's change-message log with the headers/control-messages the client
//! requires (`electric-handle`, `electric-offset`, `electric-schema`, `electric-cursor`,
//! `electric-up-to-date`; `up-to-date` / `must-refetch` control messages).
//!
//! Mapping notes:
//! - A `GET` with `offset=-1` (or no handle) is the **snapshot**: parse `where` → predicate, create a
//!   materialized shape, fold its stream to the current row set, emit every row as an `insert`, then an
//!   `up-to-date` control. The handle is the shape id; the offset is the stream's tail.
//! - A `GET` with `live=true` long-polls the stream from `offset` and emits `insert`/`update`/`delete`.
//!   Our engine emits absolute `upsert`/`delete`, so we reconstruct insert-vs-update from a per-handle
//!   key set (Electric's client rejects insert-of-existing / update-of-missing).
//! - Values are encoded as Postgres **text** (`bool`→`"true"`/`"false"`, text as-is); Electric's default
//!   value-mapper only coerces int/float, leaving these as strings — matching the oracle's stringified
//!   comparison.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::engine::Engine;
use crate::schema::{ColumnType, TableSchema};

#[derive(Debug, Deserialize)]
pub struct ShapeParams {
    table: String,
    #[serde(default)]
    offset: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    #[serde(default, rename = "where")]
    where_: Option<String>,
    #[serde(default)]
    columns: Option<String>,
    #[serde(default)]
    live: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    replica: Option<String>,
}

/// Per-handle live state: which keys the client currently holds (to pick insert vs update) and the last
/// offset we served it.
struct HandleState {
    stream_path: String,
    table: String,
    pk_name: String,
    columns: Option<Vec<String>>,
    keys: HashSet<String>,
    offset: String,
}

fn handles() -> &'static tokio::sync::Mutex<HashMap<String, HandleState>> {
    static H: OnceLock<tokio::sync::Mutex<HashMap<String, HandleState>>> = OnceLock::new();
    H.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

fn next_cursor() -> u64 {
    static C: AtomicU64 = AtomicU64::new(1);
    C.fetch_add(1, Ordering::Relaxed)
}

fn col_csv(c: &Option<String>) -> Option<Vec<String>> {
    c.as_ref().map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
}

/// A Postgres type name for the `electric-schema` header. Only `int*`/`float*` trigger the client's
/// value coercion; text/bool stay strings (which is what we want for the stringified oracle compare).
fn pg_type(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Int => "int8",
        ColumnType::Float => "float8",
        ColumnType::Text => "text",
        ColumnType::Bool => "bool",
    }
}

/// Build the `electric-schema` JSON: `{col: {type, pk_index?}}`.
fn schema_json(ts: &TableSchema, columns: &Option<Vec<String>>) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (name, ty) in &ts.columns {
        if let Some(cols) = columns {
            if !cols.iter().any(|c| c == name) && name != &ts.pk_name {
                continue;
            }
        }
        let mut entry = serde_json::Map::new();
        entry.insert("type".into(), serde_json::Value::String(pg_type(*ty).into()));
        if name == &ts.pk_name {
            entry.insert("pk_index".into(), serde_json::Value::from(0));
        }
        map.insert(name.clone(), serde_json::Value::Object(entry));
    }
    serde_json::Value::Object(map)
}

/// Re-encode a row JSON value (typed: string/bool/number/null) as Electric text values (`{col: "text"}`).
fn encode_value(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            serde_json::Value::Object(m.iter().map(|(k, val)| (k.clone(), pg_text(val))).collect())
        }
        other => other.clone(),
    }
}

fn pg_text(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Null => serde_json::Value::Null,
        serde_json::Value::Bool(b) => serde_json::Value::String(if *b { "true".into() } else { "false".into() }),
        serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
        serde_json::Value::Number(n) => serde_json::Value::String(n.to_string()),
        other => serde_json::Value::String(other.to_string()),
    }
}

fn change_msg(op: &str, key: &str, value: Option<serde_json::Value>) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    let mut headers = serde_json::Map::new();
    headers.insert("operation".into(), serde_json::Value::String(op.into()));
    m.insert("headers".into(), serde_json::Value::Object(headers));
    m.insert("key".into(), serde_json::Value::String(key.into()));
    if let Some(v) = value {
        m.insert("value".into(), v);
    }
    serde_json::Value::Object(m)
}

fn control_msg(control: &str) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    let mut headers = serde_json::Map::new();
    headers.insert("control".into(), serde_json::Value::String(control.into()));
    if control == "up-to-date" {
        headers.insert("global_last_seen_lsn".into(), serde_json::Value::String("0".into()));
    }
    m.insert("headers".into(), serde_json::Value::Object(headers));
    serde_json::Value::Object(m)
}

fn hv(s: &str) -> HeaderValue {
    HeaderValue::from_str(s).unwrap_or_else(|_| HeaderValue::from_static(""))
}

fn respond(messages: Vec<serde_json::Value>, mut headers: HeaderMap, status: StatusCode) -> Response {
    headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    (status, headers, serde_json::to_string(&messages).unwrap_or_else(|_| "[]".into())).into_response()
}

fn must_refetch() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("electric-handle"), hv("0"));
    headers.insert(HeaderName::from_static("electric-offset"), hv("-1"));
    respond(vec![control_msg("must-refetch")], headers, StatusCode::CONFLICT)
}

/// Fold a shape's whole stream (catch-up reads from `-1`) into the current key→row-value map and return
/// it with the stream's tail offset.
async fn materialize(engine: &Engine, path: &str) -> anyhow::Result<(HashMap<String, serde_json::Value>, String)> {
    let mut rows: HashMap<String, serde_json::Value> = HashMap::new();
    let mut offset = "-1".to_string();
    loop {
        let r = engine.read_shape_stream(path, &offset, false).await?;
        let empty = r.envelopes.is_empty();
        for env in r.envelopes {
            match env.headers.operation.as_str() {
                "delete" => {
                    rows.remove(&env.key);
                }
                _ => {
                    if let Some(v) = env.value {
                        rows.insert(env.key, v);
                    }
                }
            }
        }
        if let Some(n) = r.next_offset {
            offset = n;
        }
        if r.up_to_date || empty {
            break;
        }
    }
    Ok((rows, offset))
}

pub async fn shape(State(engine): State<Engine>, Query(p): Query<ShapeParams>) -> Response {
    match shape_inner(engine, p).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!("/v1/shape error: {e:#}");
            (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response()
        }
    }
}

async fn shape_inner(engine: Engine, p: ShapeParams) -> anyhow::Result<Response> {
    let offset = p.offset.clone().unwrap_or_else(|| "-1".into());
    let live = p.live.as_deref() == Some("true");
    let columns = col_csv(&p.columns);

    let Some(ts) = engine.table_schema(&p.table).await else {
        anyhow::bail!("unknown table '{}'", p.table);
    };

    // ---- Snapshot: offset=-1 (or no handle) -> create the shape and emit the current rows as inserts.
    if offset == "-1" || p.handle.is_none() {
        let pred = crate::where_sql::parse_where(p.where_.as_deref().unwrap_or(""))?;
        let rec = engine.create_shape(&p.table, pred, columns.clone(), false).await?;
        let (rows, tail) = materialize(&engine, &rec.stream_path).await?;

        let mut messages = Vec::with_capacity(rows.len() + 1);
        let mut keys = HashSet::with_capacity(rows.len());
        for (key, value) in &rows {
            messages.push(change_msg("insert", key, Some(encode_value(value))));
            keys.insert(key.clone());
        }
        messages.push(control_msg("up-to-date"));

        let schema_str = serde_json::to_string(&schema_json(&ts, &columns)).unwrap_or_default();
        handles().lock().await.insert(
            rec.id.clone(),
            HandleState {
                stream_path: rec.stream_path.clone(),
                table: p.table.clone(),
                pk_name: ts.pk_name.clone(),
                columns,
                keys,
                offset: tail.clone(),
            },
        );

        let mut headers = HeaderMap::new();
        headers.insert(HeaderName::from_static("electric-handle"), hv(&rec.id));
        headers.insert(HeaderName::from_static("electric-offset"), hv(&tail));
        headers.insert(HeaderName::from_static("electric-schema"), hv(&schema_str));
        headers.insert(HeaderName::from_static("electric-up-to-date"), hv(""));
        headers.insert(axum::http::header::CACHE_CONTROL, hv("no-store"));
        return Ok(respond(messages, headers, StatusCode::OK));
    }

    // ---- Live: long-poll from `offset`, emit insert/update/delete reconstructed against the key set.
    let handle = p.handle.clone().unwrap();
    let mut guard = handles().lock().await;
    let Some(st) = guard.get_mut(&handle) else {
        return Ok(must_refetch());
    };
    // Resync the key set if the client resumed at a different offset than we last served (rare; the
    // sequential oracle harness keeps offset == st.offset).
    if st.offset != offset {
        let (rows, _tail) = materialize(&engine, &st.stream_path).await?;
        st.keys = rows.into_keys().collect();
        st.offset = offset.clone();
    }
    let path = st.stream_path.clone();
    drop(guard); // don't hold the registry lock across the long-poll
    let r = engine.read_shape_stream(&path, &offset, live).await?;

    let mut guard = handles().lock().await;
    let Some(st) = guard.get_mut(&handle) else {
        return Ok(must_refetch());
    };
    let mut messages = Vec::new();
    for env in r.envelopes {
        match env.headers.operation.as_str() {
            "delete" => {
                if st.keys.remove(&env.key) {
                    // Electric's client requires a `value` on every change message (its parser matches
                    // on `"value"`). For a delete we carry the row's old value if present, else the key.
                    let value = env
                        .value
                        .as_ref()
                        .map(encode_value)
                        .unwrap_or_else(|| serde_json::json!({ st.pk_name.clone(): env.key }));
                    messages.push(change_msg("delete", &env.key, Some(value)));
                }
            }
            _ => {
                let value = env.value.as_ref().map(encode_value);
                if st.keys.contains(&env.key) {
                    messages.push(change_msg("update", &env.key, value));
                } else {
                    st.keys.insert(env.key.clone());
                    messages.push(change_msg("insert", &env.key, value));
                }
            }
        }
    }
    if let Some(n) = &r.next_offset {
        st.offset = n.clone();
    }
    if r.up_to_date {
        messages.push(control_msg("up-to-date"));
    }
    let served_offset = st.offset.clone();
    let _ = st.table; // (kept for diagnostics/symmetry)
    drop(guard);

    let cursor = next_cursor();
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("electric-handle"), hv(&handle));
    headers.insert(HeaderName::from_static("electric-offset"), hv(&served_offset));
    headers.insert(HeaderName::from_static("electric-cursor"), hv(&cursor.to_string()));
    if r.up_to_date {
        headers.insert(HeaderName::from_static("electric-up-to-date"), hv(""));
    }
    headers.insert(axum::http::header::CACHE_CONTROL, hv("no-store"));
    Ok(respond(messages, headers, StatusCode::OK))
}
