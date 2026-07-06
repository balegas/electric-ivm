//! Minimal durable-streams HTTP client: PUT-create, POST-append (JSON array), and
//! offset-resumable reads (catch-up + long-poll live). Offsets are opaque tokens; we just
//! persist and replay `Stream-Next-Offset`.

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
}

impl DsClient {
    pub fn new(base: impl Into<String>) -> Self {
        DsClient { base: base.into(), http: reqwest::Client::new() }
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
        let res = self
            .http
            .post(self.stream_url(path))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(envelopes)
            .send()
            .await
            .map_err(|e| AppendError::Other(anyhow::Error::new(e).context(format!("POST {path}"))))?;
        let status = res.status();
        // Drain the body so the connection can be pooled and reused (avoids a socket leak per append).
        let body = res.text().await.unwrap_or_default();
        if status.is_success() {
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
