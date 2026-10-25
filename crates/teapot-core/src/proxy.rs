//! Proxy client (A): a local HTTP proxy whose every TCP connection is carried
//! to the exit node over one long-lived iroh connection.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, bail};
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

use crate::paths::PathBook;

const MAX_HEAD: usize = 16 * 1024;

pub struct ProxyClient {
    endpoint: Endpoint,
    exit: EndpointId,
    conn: Mutex<Option<Connection>>,
    paths: PathBook,
}

impl ProxyClient {
    pub fn new(endpoint: Endpoint, exit: EndpointId, paths: PathBook) -> Self {
        Self {
            endpoint,
            exit,
            conn: Mutex::new(None),
            paths,
        }
    }

    /// Returns the live connection to the exit node, redialing if it closed.
    pub async fn connection(&self) -> anyhow::Result<Connection> {
        let mut slot = self.conn.lock().await;
        if let Some(conn) = slot.as_ref()
            && conn.close_reason().is_none()
        {
            return Ok(conn.clone());
        }
        info!(exit = %self.exit, "dialing exit node");
        let conn = self
            .endpoint
            .connect(self.exit, ALPN_PROXY)
            .await
            .context("connecting to exit node")?;
        info!(exit = %self.exit, "connected");
        self.paths.insert(&conn);
        tokio::spawn(log_path_changes(conn.clone()));
        *slot = Some(conn.clone());
        Ok(conn)
    }

    /// Opens a stream to the exit node and asks it to dial `target`.
    pub async fn open(&self, target: &str) -> anyhow::Result<(SendStream, RecvStream)> {
        let conn = self.connection().await?;
        let result = async {
            let (mut send, mut recv) = conn.open_bi().await?;
            let req = ConnectRequest {
                target: target.to_string(),
            };
            send.write_all(&req.encode()).await?;
            match ConnectResponse::parse(&read_line(&mut recv).await?)? {
                ConnectResponse::Ok => Ok((send, recv)),
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

    let (send, recv) = match client.open(&target).await {
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

    let (up, down) = tokio::io::copy_bidirectional(&mut sock, &mut tunnel).await?;
    debug!(%target, up, down, "closed");
    Ok(())
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
    fn head_rewrite_drops_hop_headers() {
        let head = "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\nProxy-Connection: keep-alive\r\nConnection: keep-alive\r\n\r\n";
        assert_eq!(
            rewrite_head(head, "GET", "/", "HTTP/1.1"),
            "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"
        );
    }
}
