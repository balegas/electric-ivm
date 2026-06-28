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
}

pub struct ReadResult {
    pub envelopes: Vec<Envelope>,
    pub next_offset: Option<String>,
    pub up_to_date: bool,
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
            .with_context(|| format!("POST {path}"))?;
        let status = res.status();
        // Drain the body so the connection can be pooled and reused (avoids a socket leak per append).
        let body = res.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(())
        } else {
            bail!("POST {path} -> {status}: {body}")
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
