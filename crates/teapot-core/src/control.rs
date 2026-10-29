//! Client for `teapot-control`: signature login, device API, presence socket.
//! Wire format in `docs/protocol.md`.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use iroh::SecretKey;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use tokio::{sync::Mutex, time::Instant};
use tokio_tungstenite::tungstenite::{self, Message, client::IntoClientRequest, http::HeaderValue};
use tracing::{debug, info, warn};

use crate::paths::{PathBook, PeerPath, PeerTraffic};

/// Cloudflare rejects library-default user agents (error 1010), so always
/// identify ourselves.
pub const USER_AGENT: &str = concat!("teapot/", env!("CARGO_PKG_VERSION"));
pub const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

const PING_EVERY: Duration = Duration::from_secs(20);
/// No message at all (not even a pong) for this long means the socket is
/// dead even if TCP has not noticed — mobile CGNAT drops idle flows silently.
/// Seen: a phone's agent stayed "connected" to nothing for 8 hours.
const SILENCE_LIMIT: Duration = Duration::from_secs(45);
const STATUS_EVERY: Duration = Duration::from_secs(5);
/// Re-send an unchanged status this often so the dashboard never goes stale.
const STATUS_REFRESH: Duration = Duration::from_secs(60);
/// Flush buffered logs to control this often. Decoupled from `STATUS_EVERY` to
/// stay inside the Cloudflare Workers free tier: one `POST /v1/logs` per device
/// per minute instead of one every 5 s (~12x fewer requests). Each cycle drains
/// the whole backlog, so a burst still arrives in full, just up to a minute late.
const LOG_PUSH_EVERY: Duration = Duration::from_secs(60);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// Cap on any single request to control — the HTTP client and the WebSocket
/// handshake alike. Without it a stalled handshake (TCP open, no RST, no
/// upgrade reply — a NAT VPS that silently drops packets) hangs the presence
/// loop forever, because `SILENCE_LIMIT` only starts once the socket connects.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(20);

/// What the presence status message carries.
type Report = (Vec<PeerPath>, Vec<PeerTraffic>, PublicIps);

/// This device's public addresses as iroh's net report sees them. Control
/// only sees the one family the presence socket happens to use.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PublicIps {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v4: Option<std::net::Ipv4Addr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v6: Option<std::net::Ipv6Addr>,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    success: bool,
    data: Option<T>,
    message: Option<String>,
    error: Option<String>,
}

/// A non-success response from control. `code` is e.g. `PENDING`.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "control: {} ({} {})",
            self.message, self.status, self.code
        )
    }
}

impl std::error::Error for ApiError {}

/// True when `err` is control saying the device still needs approval.
pub fn is_pending(err: &anyhow::Error) -> bool {
    err.downcast_ref::<ApiError>()
        .is_some_and(|e| e.code == "PENDING")
}

#[derive(Debug, Clone, Deserialize)]
pub struct Me {
    pub node_id: String,
    pub name: Option<String>,
    pub status: String,
    pub can_exit: bool,
    pub exit_enabled: bool,
    pub last_ip: Option<String>,
    #[serde(default)]
    pub public_ipv4: Option<String>,
    #[serde(default)]
    pub public_ipv6: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Peer {
    pub node_id: String,
    pub name: Option<String>,
    pub platform: Option<String>,
    pub online: bool,
    pub exit: bool,
    /// Where traffic through this peer leaves, as the peer reported it.
    #[serde(default)]
    pub public_ipv4: Option<String>,
}

impl Peer {
    pub fn label(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| self.node_id[..8].to_string())
    }

    /// Name (case-insensitive), full id, or an id prefix of at least 8 chars.
    pub fn matches(&self, wanted: &str) -> bool {
        self.node_id == wanted
            || self
                .name
                .as_deref()
                .is_some_and(|n| n.eq_ignore_ascii_case(wanted))
            || (wanted.len() >= 8 && self.node_id.starts_with(wanted))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Allowlist {
    pub exit_enabled: bool,
    pub allow: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct DeviceInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    /// `None` leaves the stored value alone, so running `proxy` on an exit
    /// node does not switch its exit off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub can_exit: Option<bool>,
}

pub struct ControlClient {
    base: String,
    http: reqwest::Client,
    key: SecretKey,
    token: Mutex<Option<String>>,
    /// Bumped whenever control says peers or allowlists changed, and on every
    /// presence (re)connect.
    changed: tokio::sync::watch::Sender<u64>,
    public_ips: std::sync::Mutex<PublicIps>,
}

impl ControlClient {
    pub fn new(base: &str, key: SecretKey) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(CONTROL_TIMEOUT)
            .build()?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            http,
            key,
            token: Mutex::new(None),
            changed: tokio::sync::watch::Sender::new(0),
            public_ips: std::sync::Mutex::new(PublicIps::default()),
        })
    }

    /// Reported with the next presence status. A new IPv4 means this device
    /// moved networks, so exit choices are re-evaluated right away.
    pub fn set_public_ips(&self, ips: PublicIps) {
        let mut current = self.public_ips.lock().expect("public ips poisoned");
        if *current != ips {
            info!(v4 = ?ips.v4, v6 = ?ips.v6, "public addresses changed");
        }
        if current.v4 != ips.v4 {
            self.changed.send_modify(|n| *n += 1);
        }
        *current = ips;
    }

    pub fn public_ips(&self) -> PublicIps {
        self.public_ips.lock().expect("public ips poisoned").clone()
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn node_id(&self) -> String {
        self.key.public().to_string()
    }

    /// Dashboard page where the owner confirms this device.
    pub fn approve_url(&self) -> String {
        format!("{}/approve/{}", self.base, self.node_id())
    }

    /// Fires after `peers_changed` or a presence reconnect.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// Proves we hold the key behind our node id; returns pending/approved.
    pub async fn login(&self) -> anyhow::Result<String> {
        #[derive(Deserialize)]
        struct Challenge {
            challenge: String,
        }
        #[derive(Deserialize)]
        struct Verified {
            token: String,
            status: String,
        }

        let node_id = self.node_id();
        let ch: Challenge = self
            .call_anon(
                Method::POST,
                "/v1/auth/challenge",
                json!({ "node_id": node_id }),
            )
            .await
            .context("requesting challenge")?;
        let signature = hex::encode(self.key.sign(ch.challenge.as_bytes()).to_bytes());
        let v: Verified = self
            .call_anon(
                Method::POST,
                "/v1/auth/verify",
                json!({ "node_id": node_id, "challenge": ch.challenge, "signature": signature }),
            )
            .await
            .context("verifying challenge")?;
        *self.token.lock().await = Some(v.token);
        Ok(v.status)
    }

    pub async fn me(&self) -> anyhow::Result<Me> {
        self.call(Method::GET, "/v1/devices/me", None).await
    }

    pub async fn update_me(&self, info: &DeviceInfo) -> anyhow::Result<Me> {
        self.call(
            Method::PUT,
            "/v1/devices/me",
            Some(serde_json::to_value(info)?),
        )
        .await
    }

    pub async fn peers(&self) -> anyhow::Result<Vec<Peer>> {
        self.call(Method::GET, "/v1/peers", None).await
    }

    pub async fn allowlist(&self) -> anyhow::Result<Allowlist> {
        self.call(Method::GET, "/v1/exit/allowlist", None).await
    }

    pub async fn upload_logs(&self, entries: &[crate::logs::LogEntry]) -> anyhow::Result<()> {
        let _: serde_json::Value = self
            .call(Method::POST, "/v1/logs", Some(json!({"entries": entries})))
            .await?;
        Ok(())
    }

    pub async fn logs(
        &self,
        device: Option<&str>,
        level: &str,
    ) -> anyhow::Result<Vec<crate::logs::StoredLog>> {
        #[derive(Deserialize)]
        struct History {
            entries: Vec<crate::logs::StoredLog>,
        }
        let mut url = reqwest::Url::parse("https://control/v1/logs")?;
        url.query_pairs_mut().append_pair("level", level);
        if let Some(device) = device {
            url.query_pairs_mut().append_pair("device", device);
        }
        let result: History = self
            .call(
                Method::GET,
                &format!("/v1/logs?{}", url.query().unwrap_or_default()),
                None,
            )
            .await?;
        Ok(result.entries)
    }

    /// One viewer socket, independent of presence. Caller reconnects using cursor.
    pub async fn follow_logs(
        &self,
        device: Option<&str>,
        level: &str,
        cursor: &mut u64,
        mut emit: impl FnMut(crate::logs::StoredLog),
    ) -> anyhow::Result<()> {
        let mut url = reqwest::Url::parse(&format!(
            "{}/v1/logs/stream",
            self.base
                .replacen("https://", "wss://", 1)
                .replacen("http://", "ws://", 1)
        ))?;
        url.query_pairs_mut().append_pair("level", level);
        if let Some(device) = device {
            url.query_pairs_mut().append_pair("device", device);
        }
        if *cursor > 0 {
            url.query_pairs_mut()
                .append_pair("after", &cursor.to_string());
        }
        let mut req = url.as_str().into_client_request()?;
        req.headers_mut().insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {}", self.token().await?))?,
        );
        req.headers_mut()
            .insert("user-agent", HeaderValue::from_static(USER_AGENT));
        let (mut ws, _) = match connect_ws(req, CONTROL_TIMEOUT).await {
            Ok(ok) => ok,
            Err(tungstenite::Error::Http(res)) if res.status() == StatusCode::UNAUTHORIZED => {
                *self.token.lock().await = None;
                bail!("log viewer token expired");
            }
            Err(e) => return Err(e.into()),
        };
        let mut ping = tokio::time::interval(PING_EVERY);
        let mut heard = Instant::now();
        loop {
            tokio::select! {
                _ = ping.tick() => {
                    if heard.elapsed() > SILENCE_LIMIT { bail!("log stream silent; reconnecting"); }
                    ws.send(Message::text("ping")).await?;
                }
                message = ws.next() => {
                    heard = Instant::now();
                    match message {
                        Some(Ok(Message::Text(text))) if text.as_str() != "pong" => {
                            #[derive(Deserialize)]
                            struct Batch { entries: Vec<crate::logs::StoredLog> }
                            let batch: Batch = serde_json::from_str(&text)?;
                            for e in batch.entries { if e.cursor > *cursor { *cursor = e.cursor; emit(e); } }
                        }
                        Some(Ok(Message::Close(_))) | None => bail!("log stream closed"),
                        Some(Err(e)) => return Err(e.into()),
                        _ => {}
                    }
                }
            }
        }
    }

    async fn token(&self) -> anyhow::Result<String> {
        if let Some(t) = self.token.lock().await.clone() {
            return Ok(t);
        }
        self.login().await?;
        Ok(self
            .token
            .lock()
            .await
            .clone()
            .expect("login stores a token"))
    }

    /// Authenticated call; on 401 logs in again once (expired token, or the
    /// device was removed and has to re-register).
    async fn call<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<T> {
        for attempt in 0..2 {
            let token = self.token().await?;
            let mut req = self
                .http
                .request(method.clone(), format!("{}{path}", self.base))
                .bearer_auth(token);
            if let Some(body) = &body {
                req = req.json(body);
            }
            let res = req.send().await?;
            if res.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                *self.token.lock().await = None;
                continue;
            }
            return parse(res).await;
        }
        unreachable!("second attempt always returns")
    }

    async fn call_anon<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<T> {
        let res = self
            .http
            .request(method, format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await?;
        parse(res).await
    }

    /// Keeps the presence socket up forever, reporting `paths`.
    pub async fn run_presence(self: Arc<Self>, paths: PathBook) {
        let presence = self.clone();
        tokio::join!(presence.presence_loop(paths), async {
            loop {
                tokio::time::sleep(LOG_PUSH_EVERY).await;
                if let Some(buffer) = crate::logs::buffer() {
                    // Drain the whole backlog each cycle: `batch` caps at 20, so a
                    // busy minute needs several POSTs, but a quiet agent sends one.
                    loop {
                        let entries = buffer.batch();
                        if entries.is_empty() {
                            break;
                        }
                        if self.upload_logs(&entries).await.is_ok() {
                            buffer.ack(&entries);
                        } else {
                            break; // control unreachable; retry next cycle
                        }
                    }
                }
            }
        });
    }

    async fn presence_loop(self: Arc<Self>, paths: PathBook) {
        let mut backoff = Duration::from_secs(1);
        loop {
            let started = Instant::now();
            match self.presence_once(&paths).await {
                Ok(reason) => info!("presence closed: {reason}"),
                Err(e) => warn!("presence: {e:#}"),
            }
            if started.elapsed() > Duration::from_secs(30) {
                backoff = Duration::from_secs(1);
            }
            let delay = backoff.mul_f64(0.5 + rand::random::<f64>() * 0.5);
            info!(retry_ms = delay.as_millis(), "presence reconnect scheduled");
            tokio::time::sleep(delay).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }

    async fn presence_once(&self, paths: &PathBook) -> anyhow::Result<String> {
        let url = format!(
            "{}/v1/presence",
            self.base
                .replacen("https://", "wss://", 1)
                .replacen("http://", "ws://", 1)
        );
        let mut req = url.into_client_request()?;
        let headers = req.headers_mut();
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {}", self.token().await?))?,
        );
        headers.insert("user-agent", HeaderValue::from_static(USER_AGENT));
        headers.insert("x-teapot-version", HeaderValue::from_static(AGENT_VERSION));
        headers.insert("x-teapot-protocol", HeaderValue::from_static("1"));

        let (ws, _) = match connect_ws(req, CONTROL_TIMEOUT).await {
            Ok(ok) => ok,
            Err(tungstenite::Error::Http(res)) if res.status() == StatusCode::UNAUTHORIZED => {
                *self.token.lock().await = None;
                bail!("token rejected; logging in again");
            }
            Err(e) => return Err(e.into()),
        };
        info!("presence connected");
        self.changed.send_modify(|n| *n += 1);

        let (mut tx, mut rx) = ws.split();
        let mut ping = tokio::time::interval(PING_EVERY);
        let mut status = tokio::time::interval(STATUS_EVERY);
        let mut last_sent: Option<(Report, Instant)> = None;
        let mut last_heard = Instant::now();

        loop {
            tokio::select! {
                _ = ping.tick() => {
                    if last_heard.elapsed() > SILENCE_LIMIT {
                        bail!("no reply from control for {:?}; reconnecting", last_heard.elapsed());
                    }
                    tx.send(Message::text("ping")).await?
                }
                _ = status.tick() => {
                    let now = (paths.snapshot(), paths.traffic(), self.public_ips());
                    let due = match &last_sent {
                        Some((prev, at)) => *prev != now || at.elapsed() >= STATUS_REFRESH,
                        None => true,
                    };
                    if due {
                        let msg = json!({ "type": "status", "paths": now.0, "traffic": now.1, "ips": now.2 });
                        tx.send(Message::text(msg.to_string())).await?;
                        last_sent = Some((now, Instant::now()));
                    }
                }
                msg = rx.next() => {
                    if matches!(msg, Some(Ok(_))) {
                        last_heard = Instant::now();
                    }
                    match msg {
                    None => return Ok("stream ended".into()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Close(frame))) => {
                        return Ok(frame
                            .map(|f| format!("{} {}", u16::from(f.code), f.reason))
                            .unwrap_or_else(|| "no reason".into()));
                    }
                    Some(Ok(Message::Text(text))) => {
                        if text.as_str() == "pong" {
                            continue;
                        }
                        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(&text) {
                            match msg["type"].as_str() {
                                Some("peers_changed") => {
                                    debug!("control: peers changed");
                                    self.changed.send_modify(|n| *n += 1);
                                }
                                Some("hello") => info!(
                                    protocol = ?msg["protocol"],
                                    control_version = ?msg["control_version"],
                                    generation = ?msg["presence_generation"],
                                    agent_version = AGENT_VERSION,
                                    "control handshake"
                                ),
                                _ => {}
                            }
                        }
                    }
                    Some(Ok(_)) => {}
                    }
                }
            }
        }
    }
}

/// Open the presence/log WebSocket, giving up after `timeout` instead of
/// hanging forever when control accepts the TCP connection but never finishes
/// the upgrade. The timeout comes back as an `Io`/`TimedOut` error, so the
/// callers' existing error path logs it and the loop reconnects.
async fn connect_ws(
    req: tungstenite::handshake::client::Request,
    timeout: Duration,
) -> tungstenite::Result<(
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tungstenite::handshake::client::Response,
)> {
    match tokio::time::timeout(timeout, tokio_tungstenite::connect_async(req)).await {
        Ok(result) => result,
        Err(_) => Err(tungstenite::Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("control websocket handshake exceeded {timeout:?}"),
        ))),
    }
}

async fn parse<T: DeserializeOwned>(res: reqwest::Response) -> anyhow::Result<T> {
    let status = res.status();
    let text = res.text().await?;
    let env: Envelope<T> = serde_json::from_str(&text).map_err(|_| ApiError {
        status: status.as_u16(),
        code: "BAD_RESPONSE".into(),
        message: text.chars().take(200).collect(),
    })?;
    match (env.success, env.data) {
        (true, Some(data)) => Ok(data),
        _ => Err(ApiError {
            status: status.as_u16(),
            code: env.error.unwrap_or_else(|| "ERROR".into()),
            message: env.message.unwrap_or_else(|| "request failed".into()),
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A handshake to a peer that accepts the TCP connection but never answers
    /// the WebSocket upgrade must fail within the timeout, not hang forever.
    #[tokio::test]
    async fn stalled_handshake_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept connections and hold them open without ever replying.
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });

        let req = format!("ws://{addr}/v1/presence")
            .into_client_request()
            .unwrap();
        let timeout = Duration::from_millis(300);
        let started = Instant::now();
        let err = connect_ws(req, timeout).await.unwrap_err();

        assert!(
            matches!(err, tungstenite::Error::Io(e) if e.kind() == std::io::ErrorKind::TimedOut)
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "handshake should give up near the timeout, took {:?}",
            started.elapsed()
        );
    }
}
