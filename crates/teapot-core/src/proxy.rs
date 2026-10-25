//! Proxy client (A): a local HTTP proxy whose every TCP connection is carried
//! to an exit node over a long-lived iroh connection.
//!
//! The exit is the agent's default one, or the one named in the proxy
//! username (`http://quantumz@127.0.0.1:1080`), which clients send as
//! `Proxy-Authorization` — on CONNECT too, unlike custom headers that sit
//! inside the TLS tunnel. `GET /_teapot/exits` lists what can be named.

use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, anyhow, bail};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use teapot_proto::{ALPN_PROXY, ConnectRequest, ConnectResponse, read_line};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tracing::{debug, info, warn};

use crate::{
    control::Peer,
    paths::{Counted, PathBook},
};

const MAX_HEAD: usize = 16 * 1024;
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

pub struct ProxyClient {
    endpoint: Endpoint,
    /// Held only briefly, never across a dial, so switching exit nodes is
    /// never stuck behind a dial to one that just went away.
    state: Mutex<State>,
    /// One dial at a time; concurrent requests wait for it and share the result.
    dial: Mutex<()>,
    paths: PathBook,
    exit_tx: tokio::sync::watch::Sender<Option<EndpointId>>,
    /// Connections to exits named per request (not the default one).
    explicit: Mutex<HashMap<EndpointId, Connection>>,
    /// Latest approved peers from control, for resolving requested exits.
    peers: std::sync::Mutex<Vec<Peer>>,
}

/// The chosen exit node and the connection to it, swapped together.
#[derive(Default)]
struct State {
    exit: Option<EndpointId>,
    conn: Option<Connection>,
}

impl ProxyClient {
    pub fn new(endpoint: Endpoint, exit: Option<EndpointId>, paths: PathBook) -> Self {
        Self {
            endpoint,
            state: Mutex::new(State { exit, conn: None }),
            dial: Mutex::new(()),
            paths,
            exit_tx: tokio::sync::watch::Sender::new(exit),
            explicit: Mutex::new(HashMap::new()),
            peers: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn set_peers(&self, peers: Vec<Peer>) {
        *self.peers.lock().expect("peers poisoned") = peers;
    }

    /// A requested exit by name, id, or id prefix. It must be an enabled,
    /// online exit: a caller who names one wants that one, not a fallback.
    fn resolve(&self, wanted: &str) -> anyhow::Result<EndpointId> {
        let peers = self.peers.lock().expect("peers poisoned");
        if let Some(p) = peers.iter().find(|p| p.matches(wanted)) {
            if !p.exit {
                bail!("{} is not an enabled exit node", p.label());
            }
            if !p.online {
                bail!("{} is offline", p.label());
            }
            return Ok(EndpointId::from_str(&p.node_id)?);
        }
        // Without control (--no-control) only full ids work.
        EndpointId::from_str(wanted).map_err(|_| anyhow!("no exit node matches {wanted:?}"))
    }

    /// Connection to a specific exit, reusing the default one when it is the same.
    async fn connection_to(&self, exit: EndpointId) -> anyhow::Result<Connection> {
        if self.exit().await == Some(exit) {
            return self.connection().await;
        }
        let mut conns = self.explicit.lock().await;
        if let Some(conn) = conns.get(&exit).filter(|c| c.close_reason().is_none()) {
            return Ok(conn.clone());
        }
        info!(%exit, "dialing requested exit node");
        let conn = tokio::time::timeout(DIAL_TIMEOUT, self.endpoint.connect(exit, ALPN_PROXY))
            .await
            .map_err(|_| anyhow!("exit node did not answer within {DIAL_TIMEOUT:?}"))?
            .context("connecting to exit node")?;
        self.paths.insert(&conn);
        tokio::spawn(log_path_changes(conn.clone()));
        conns.insert(exit, conn.clone());
        Ok(conn)
    }

    /// JSON for `GET /_teapot/exits`.
    pub async fn exits_json(&self) -> String {
        let default = self.exit().await.map(|id| id.to_string());
        let peers = self.peers.lock().expect("peers poisoned").clone();
        let exits: Vec<serde_json::Value> = peers
            .iter()
            .filter(|p| p.exit)
            .map(|p| {
                serde_json::json!({
                    "name": p.label(),
                    "node_id": p.node_id,
                    "online": p.online,
                    "public_ipv4": p.public_ipv4,
                    "default": default.as_deref() == Some(p.node_id.as_str()),
                })
            })
            .collect();
        serde_json::json!({ "exits": exits }).to_string()
    }

    /// Fires whenever the chosen exit node changes (including to none).
    pub fn watch_exit(&self) -> tokio::sync::watch::Receiver<Option<EndpointId>> {
        self.exit_tx.subscribe()
    }

    pub async fn exit(&self) -> Option<EndpointId> {
        self.state.lock().await.exit
    }

    /// Points new streams at another exit node (or none). Streams on the old
    /// connection are cut. Returns whether anything changed.
    pub async fn set_exit(&self, exit: Option<EndpointId>) -> bool {
        let mut state = self.state.lock().await;
        if state.exit == exit {
            return false;
        }
        if let Some(old) = state.conn.take() {
            old.close(0u32.into(), b"switching exit node");
        }
        state.exit = exit;
        self.exit_tx.send_replace(exit);
        true
    }

    /// Returns the live connection to the exit node, redialing if it closed.
    pub async fn connection(&self) -> anyhow::Result<Connection> {
        if let Some(conn) = self.live().await? {
            return Ok(conn);
        }
        let _dialing = self.dial.lock().await;
        // Someone may have dialed while we waited.
        if let Some(conn) = self.live().await? {
            return Ok(conn);
        }
        let exit = self.exit().await.context("no exit node online")?;

        info!(%exit, "dialing exit node");
        let conn = tokio::time::timeout(DIAL_TIMEOUT, self.endpoint.connect(exit, ALPN_PROXY))
            .await
            .map_err(|_| anyhow::anyhow!("exit node did not answer within {DIAL_TIMEOUT:?}"))?
            .context("connecting to exit node")?;

        let mut state = self.state.lock().await;
        if state.exit != Some(exit) {
            conn.close(0u32.into(), b"switching exit node");
            bail!("exit node changed while dialing; retry");
        }
        info!(%exit, "connected");
        self.paths.insert(&conn);
        tokio::spawn(log_path_changes(conn.clone()));
        state.conn = Some(conn.clone());
        Ok(conn)
    }

    /// The open connection, if any; errors when no exit node is chosen.
    async fn live(&self) -> anyhow::Result<Option<Connection>> {
        let state = self.state.lock().await;
        if state.exit.is_none() {
            bail!("no exit node online");
        }
        Ok(state
            .conn
            .as_ref()
            .filter(|c| c.close_reason().is_none())
            .cloned())
    }

    /// Opens a stream to the exit node and asks it to dial `target`.
    /// Returns the streams and which exit node they go to.
    pub async fn open(&self, target: &str) -> anyhow::Result<(SendStream, RecvStream, EndpointId)> {
        self.open_via(target, None).await
    }

    /// Like [`open`](Self::open) through the named exit, or the default one.
    pub async fn open_via(
        &self,
        target: &str,
        via: Option<&str>,
    ) -> anyhow::Result<(SendStream, RecvStream, EndpointId)> {
        let conn = match via {
            None => self.connection().await?,
            Some(wanted) => self.connection_to(self.resolve(wanted)?).await?,
        };
        let exit = conn.remote_id();
        let result = async {
            let (mut send, mut recv) = conn.open_bi().await?;
            let req = ConnectRequest {
                target: target.to_string(),
            };
            send.write_all(&req.encode()).await?;
            match ConnectResponse::parse(&read_line(&mut recv).await?)? {
                ConnectResponse::Ok => Ok((send, recv, exit)),
                ConnectResponse::Err(reason) => bail!("exit node refused {target}: {reason}"),
            }
        }
        .await;
        // A stream error after the exit node closed us (e.g. allowlist) says
        // only "connection lost"; the close reason is the useful part.
        if result.is_err()
            && let Some(reason) = conn.close_reason()
        {
            bail!("exit node closed the connection: {reason}");
        }
        result
    }
}

/// Logs whenever the selected path flips between direct and relay.
async fn log_path_changes(conn: Connection) {
    let mut last = String::new();
    while conn.close_reason().is_none() {
        let now = conn
            .paths()
            .iter()
            .find(|p| p.is_selected())
            .map(|p| {
                let kind = if p.is_relay() { "relay" } else { "direct" };
                format!("{kind} {:?} rtt={}ms", p.remote_addr(), p.rtt().as_millis())
            })
            .unwrap_or_else(|| "no path".into());
        // Only the kind and address matter for change detection; rtt jitters.
        let key: String = now.split(" rtt=").next().unwrap_or_default().to_string();
        if key != last {
            info!("path: {now}");
            last = key;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

pub async fn serve(listen: SocketAddr, client: Arc<ProxyClient>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    info!("HTTP proxy listening on {listen}");
    serve_listener(listener, client).await
}

pub async fn serve_listener(listener: TcpListener, client: Arc<ProxyClient>) -> anyhow::Result<()> {
    loop {
        let (sock, from) = listener.accept().await?;
        let client = client.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_http(sock, &client).await {
                warn!(%from, "{e:#}");
            }
        });
    }
}

async fn handle_http(mut sock: TcpStream, client: &ProxyClient) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let head_end = loop {
        let mut chunk = [0u8; 4096];
        let n = sock.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        if buf.len() > MAX_HEAD {
            bail!("request head larger than {MAX_HEAD} bytes");
        }
    };
    let head = std::str::from_utf8(&buf[..head_end]).context("non-utf8 request head")?;
    let request_line = head.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(uri), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        sock.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await?;
        bail!("bad request line: {request_line:?}");
    };

    // Requests to the proxy itself, not through it.
    if uri.starts_with("/_teapot/") {
        let (status, body) = match uri {
            "/_teapot/exits" => ("200 OK", client.exits_json().await),
            _ => ("404 Not Found", r#"{"error":"not found"}"#.to_string()),
        };
        let resp = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(resp.as_bytes()).await?;
        return Ok(());
    }
    let via = requested_exit(head);

    // CONNECT host:port (HTTPS and anything else) tunnels raw bytes.
    // Absolute-form `GET http://host/...` (plain HTTP) is rewritten to
    // origin-form and forwarded with Connection: close.
    let (target, preamble) = if method.eq_ignore_ascii_case("CONNECT") {
        (uri.to_string(), None)
    } else {
        let Some((target, path)) = split_absolute_uri(uri) else {
            sock.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await?;
            bail!("expected absolute http:// URI, got {uri:?}");
        };
        (target, Some(rewrite_head(head, method, &path, version)))
    };
    let rest = &buf[head_end..];

    let (send, recv, exit) = match client.open_via(&target, via.as_deref()).await {
        Ok(streams) => streams,
        Err(e) => {
            let body = format!("{e:#}\n");
            let resp = format!(
                "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.ok();
            return Err(e);
        }
    };
    let mut tunnel = tokio::io::join(recv, send);

    let preamble_len = preamble.as_ref().map_or(0, String::len);
    match preamble {
        None => {
            sock.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await?
        }
        Some(head) => tunnel.write_all(head.as_bytes()).await?,
    }
    if !rest.is_empty() {
        tunnel.write_all(rest).await?;
    }

    let counter = client.paths.counter(exit);
    counter.add_tx(rest.len() + preamble_len);
    let mut sock = Counted::new(sock, counter);
    let (up, down) = tokio::io::copy_bidirectional(&mut sock, &mut tunnel).await?;
    debug!(%target, up, down, "closed");
    Ok(())
}

/// The exit named as the proxy username (`Proxy-Authorization: Basic
/// base64("name:")`); the password part is ignored.
fn requested_exit(head: &str) -> Option<String> {
    let value = head.lines().skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case("proxy-authorization")
            .then(|| value.trim())
    })?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = String::from_utf8(BASE64.decode(token.trim()).ok()?).ok()?;
    let user = decoded.split(':').next()?.trim();
    (!user.is_empty()).then(|| user.to_string())
}

/// `http://host[:port]/path?q` -> (`host:port`, `/path?q`).
fn split_absolute_uri(uri: &str) -> Option<(String, String)> {
    let rest = uri.strip_prefix("http://")?;
    let (authority, path) = match rest.find(['/', '?']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return None;
    }
    let has_port = match authority.rfind(']') {
        Some(close) => authority[close..].contains(':'), // [v6]:port
        None => authority.contains(':'),
    };
    let target = if has_port {
        authority.to_string()
    } else {
        format!("{authority}:80")
    };
    let path = if path.starts_with('?') {
        format!("/{path}")
    } else {
        path.to_string()
    };
    Some((target, path))
}

fn rewrite_head(head: &str, method: &str, path: &str, version: &str) -> String {
    let mut out = format!("{method} {path} {version}\r\n");
    for line in head.lines().skip(1) {
        let name = line.split(':').next().unwrap_or_default().trim();
        if line.is_empty()
            || name.eq_ignore_ascii_case("proxy-connection")
            || name.eq_ignore_ascii_case("proxy-authorization")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        out.push_str(line);
        out.push_str("\r\n");
    }
    // One request per tunnel keeps keep-alive from leaking requests for other
    // hosts into this stream.
    out.push_str("Connection: close\r\n\r\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_uri() {
        let cases = [
            ("http://example.com/a?b", "example.com:80", "/a?b"),
            ("http://example.com", "example.com:80", "/"),
            ("http://example.com:8080/x", "example.com:8080", "/x"),
            ("http://example.com?q=1", "example.com:80", "/?q=1"),
            ("http://[::1]:8080/", "[::1]:8080", "/"),
            ("http://[::1]/", "[::1]:80", "/"),
        ];
        for (uri, target, path) in cases {
            assert_eq!(
                split_absolute_uri(uri),
                Some((target.to_string(), path.to_string())),
                "{uri}"
            );
        }
        assert_eq!(split_absolute_uri("https://example.com/"), None);
    }

    #[test]
    fn exit_from_proxy_username() {
        let head =
            |auth: &str| format!("CONNECT a:443 HTTP/1.1\r\nProxy-Authorization: {auth}\r\n\r\n");
        // base64("quantumz:") and base64("quantumz") — both forms clients send
        assert_eq!(
            requested_exit(&head("Basic cXVhbnR1bXo6")).as_deref(),
            Some("quantumz")
        );
        assert_eq!(
            requested_exit(&head("basic cXVhbnR1bXo=")).as_deref(),
            Some("quantumz")
        );
        assert_eq!(requested_exit(&head("Bearer xyz")), None);
        assert_eq!(
            requested_exit("CONNECT a:443 HTTP/1.1\r\nHost: a\r\n\r\n"),
            None
        );
    }

    #[test]
    fn head_rewrite_drops_hop_headers() {
        let head = "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\nProxy-Connection: keep-alive\r\nProxy-Authorization: Basic cXVhbnR1bXo6\r\nConnection: keep-alive\r\n\r\n";
        assert_eq!(
            rewrite_head(head, "GET", "/", "HTTP/1.1"),
            "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"
        );
    }
}
