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

const PING_EVERY: Duration = Duration::from_secs(20);
const STATUS_EVERY: Duration = Duration::from_secs(5);
/// Re-send an unchanged status this often so the dashboard never goes stale.
const STATUS_REFRESH: Duration = Duration::from_secs(60);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

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
            .timeout(Duration::from_secs(20))
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
            tokio::time::sleep(backoff).await;
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

        let (ws, _) = match tokio_tungstenite::connect_async(req).await {
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

        loop {
            tokio::select! {
                _ = ping.tick() => tx.send(Message::text("ping")).await?,
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
                msg = rx.next() => match msg {
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
                        if text.contains("\"peers_changed\"") {
                            debug!("control: peers changed");
                            self.changed.send_modify(|n| *n += 1);
                        }
                    }
                    Some(Ok(_)) => {}
                },
            }
        }
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
