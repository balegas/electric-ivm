//! Minimal durable-streams HTTP client: PUT-create, POST-append (JSON array), and
//! offset-resumable reads (catch-up + long-poll live). Offsets are opaque tokens; we just
//! persist and replay `Stream-Next-Offset`.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// A State-Protocol change event, the JSON item on every table/shape stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "type")]
    pub type_: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// The full prior row, carried by replication on UPDATE/DELETE (`REPLICA IDENTITY FULL`). Lets
    /// the engine compute the input delta without an in-memory `table_state`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old: Option<serde_json::Value>,
    pub headers: EnvelopeHeaders,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeHeaders {
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txid: Option<String>,
    // The server stamps an `offset` onto each item; accept it on read, never send it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    /// Postgres commit LSN of the change (set by the replication ingestor). Used to skip changes a
    /// shape/family already reflects from its backfill snapshot (`lsn <= seed_lsn`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<String>,
    /// Position of this change within its transaction (set by the ingestor). `(lsn, seq)` uniquely
    /// identifies a change, letting the tailer skip duplicates when the ingestor re-appends a batch
    /// after a partial failure or a crash between append and slot-advance (at-least-once delivery).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}

pub struct ReadResult {
    pub envelopes: Vec<Envelope>,
    pub next_offset: Option<String>,
    pub up_to_date: bool,
}

/// Why an append failed: the stream no longer exists (shape dropped — discard), or a transient/other
/// error (retry or surface).
enum AppendError {
    Gone,
    Other(anyhow::Error),
}

#[derive(Clone)]
pub struct DsClient {
    base: String,
    http: reqwest::Client,
    /// Bytes appended per stream path since this process started (serialized request bodies).
    /// The durable-streams server exposes no per-stream sizes, so this engine-side accounting is
    /// what the retention disk-budget layer works from. It undercounts streams that already
    /// existed before the process started (restart persistence is the catalog work, GH #8).
    appended: std::sync::Arc<std::sync::Mutex<HashMap<String, u64>>>,
}

impl DsClient {
    pub fn new(base: impl Into<String>) -> Self {
        DsClient {
            base: base.into(),
            http: reqwest::Client::new(),
            appended: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Tracked bytes appended to `path` since process start (0 if never appended).
    pub fn appended_bytes(&self, path: &str) -> u64 {
        self.appended.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    /// Snapshot of tracked appended bytes for every stream path with the given prefix
    /// (e.g. `"shape/"` for the retention disk budget).
    pub fn appended_bytes_with_prefix(&self, prefix: &str) -> HashMap<String, u64> {
        self.appended
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.starts_with(prefix))
            .map(|(p, b)| (p.clone(), *b))
            .collect()
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn stream_url(&self, path: &str) -> String {
        format!("{}/{}", self.base.trim_end_matches('/'), path.trim_start_matches('/'))
    }

    /// Idempotently create a JSON stream (PUT). Existing stream with same config -> 200.
    pub async fn ensure_stream(&self, path: &str) -> Result<()> {
        let res = self
            .http
            .put(self.stream_url(path))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .send()
            .await
            .with_context(|| format!("PUT {path}"))?;
        let status = res.status();
        // Drain the body so the connection returns to reqwest's pool. Skipping this leaks one socket
        // per call, which exhausts ephemeral ports when creating many shape streams.
        let body = res.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(())
        } else {
            bail!("PUT {path} -> {status}: {body}")
        }
    }

    /// Append envelopes as a JSON array (the server flattens one array level into N messages).
    /// Append raw JSON events (non-envelope streams, e.g. the shape catalog).
    pub async fn append_json(&self, path: &str, events: &[serde_json::Value]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let res = self
            .http
            .post(self.stream_url(path))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(events)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        let status = res.status();
        if !status.is_success() {
            let body = res.text().await.unwrap_or_default();
            bail!("POST {path} -> {status}: {body}");
        }
        Ok(())
    }

    /// Read raw JSON events (non-envelope streams). Returns `(events, next_offset, up_to_date)`.
    pub async fn read_json(
        &self,
        path: &str,
        offset: &str,
    ) -> Result<(Vec<serde_json::Value>, Option<String>, bool)> {
        let url = format!("{}?offset={}", self.stream_url(path), offset);
        let res = self.http.get(url).send().await.with_context(|| format!("GET {path}"))?;
        let status = res.status();
        let next_offset = header(&res, "stream-next-offset");
        let up_to_date = res.headers().get("stream-up-to-date").is_some();
        if status.as_u16() == 204 || status.as_u16() == 404 {
            return Ok((Vec::new(), next_offset, true));
        }
        if !status.is_success() {
            bail!("GET {path} -> {status}");
        }
        let body = res.text().await?;
        let events: Vec<serde_json::Value> = if body.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&body).with_context(|| format!("parsing stream body: {body}"))?
        };
        Ok((events, next_offset, up_to_date))
    }

    pub async fn append(&self, path: &str, envelopes: &[Envelope]) -> Result<()> {
        match self.append_once(path, envelopes).await {
            Ok(()) => Ok(()),
            Err(AppendError::Gone) => bail!("POST {path} -> 404 (stream gone)"),
            Err(AppendError::Other(e)) => Err(e),
        }
    }

    async fn append_once(&self, path: &str, envelopes: &[Envelope]) -> std::result::Result<(), AppendError> {
        if envelopes.is_empty() {
            return Ok(());
        }
        // Serialize once ourselves (instead of `.json(...)`) so the successful append's byte size
        // can be recorded for the retention disk-budget accounting.
        let payload = serde_json::to_vec(envelopes)
            .map_err(|e| AppendError::Other(anyhow::Error::new(e).context(format!("serializing POST {path}"))))?;
        let payload_len = payload.len() as u64;
        let res = self
            .http
            .post(self.stream_url(path))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload)
            .send()
            .await
            .map_err(|e| AppendError::Other(anyhow::Error::new(e).context(format!("POST {path}"))))?;
        let status = res.status();
        // Drain the body so the connection can be pooled and reused (avoids a socket leak per append).
        let body = res.text().await.unwrap_or_default();
        if status.is_success() {
            *self.appended.lock().unwrap().entry(path.to_string()).or_insert(0) += payload_len;
            Ok(())
        } else if status.as_u16() == 404 {
            Err(AppendError::Gone)
        } else {
            Err(AppendError::Other(anyhow::anyhow!("POST {path} -> {status}: {body}")))
        }
    }

    /// Append with **no silent loss**: retry transient failures with capped backoff until the append
    /// lands. A dropped shape-stream append is a permanent divergence for every subscriber of that
    /// shape, so the only sound behaviors are (a) retry until success — the storage server being down
    /// simply backpressures the tailer, matching the ingestor's read-then-commit stance — or (b) stop
    /// because the stream was deleted (the shape was dropped mid-flush), which is a clean no-op.
    /// Envelopes are absolute per-pk (`upsert`/`delete` by key), so an at-least-once retry that
    /// double-appends after an ambiguous network failure is idempotent for readers.
    /// Returns `false` iff the stream is gone (404).
    pub async fn append_reliable(&self, path: &str, envelopes: &[Envelope]) -> bool {
        let mut attempt = 0u32;
        loop {
            match self.append_once(path, envelopes).await {
                Ok(()) => return true,
                Err(AppendError::Gone) => {
                    tracing::debug!("append to {path}: stream gone (shape dropped); discarding {} envelopes", envelopes.len());
                    return false;
                }
                Err(AppendError::Other(e)) => {
                    attempt += 1;
                    let backoff = std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
                    if attempt.is_multiple_of(10) {
                        tracing::error!("append to {path} still failing after {attempt} attempts: {e:#}");
                    } else {
                        tracing::warn!("append to {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}");
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Delete a stream (DELETE). Absent stream (404) is a success — deletion is idempotent.
    pub async fn delete_stream(&self, path: &str) -> Result<()> {
        let res = self.http.delete(self.stream_url(path)).send().await.with_context(|| format!("DELETE {path}"))?;
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        if status.is_success() || status.as_u16() == 404 {
            self.appended.lock().unwrap().remove(path);
            Ok(())
        } else {
            bail!("DELETE {path} -> {status}: {body}")
        }
    }

    /// Read from `offset` (use "-1" for the beginning). `live` enables long-poll tailing.
    pub async fn read(&self, path: &str, offset: &str, live: bool) -> Result<ReadResult> {
        let mut url = format!("{}?offset={}", self.stream_url(path), offset);
        if live {
            url.push_str("&live=long-poll");
        }
        let res = self.http.get(url).send().await.with_context(|| format!("GET {path}"))?;
        let status = res.status();
        let next_offset = header(&res, "stream-next-offset");
        let up_to_date = res.headers().get("stream-up-to-date").is_some();

        // 204 = long-poll timeout / no new data.
        if status.as_u16() == 204 {
            return Ok(ReadResult { envelopes: Vec::new(), next_offset, up_to_date });
        }
        if !status.is_success() {
            bail!("GET {path} -> {status}");
        }
        let body = res.text().await?;
        let envelopes: Vec<Envelope> = if body.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&body).with_context(|| format!("parsing stream body: {body}"))?
        };
        Ok(ReadResult { envelopes, next_offset, up_to_date })
    }
}

fn header(res: &reqwest::Response, name: &str) -> Option<String> {
    res.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}
