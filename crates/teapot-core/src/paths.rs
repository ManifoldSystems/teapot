//! Per-peer view of live iroh connections for the presence report: which path
//! (direct or relay) each uses, and how many tunnel bytes went each way.

use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use iroh::{EndpointId, TransportAddr, endpoint::Connection};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeerPath {
    pub peer: String,
    pub kind: &'static str,
    pub rtt_ms: u64,
    /// Remote address of the selected path: the peer's socket address when
    /// direct, the relay host when relay. `None` when iroh exposes no address.
    /// Optional on the wire so legacy readers and senders stay valid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
}

/// The selected path's remote address as a human-readable string: `ip:port`
/// for a direct path, the relay host for a relayed one. `None` when the
/// address is a shape we don't render (e.g. a custom transport).
fn path_addr(remote: &TransportAddr) -> Option<String> {
    match remote {
        TransportAddr::Ip(addr) => Some(addr.to_string()),
        // The relay host alone; the scheme/port add no signal for the dashboard.
        TransportAddr::Relay(url) => url.host_str().map(str::to_string),
        _ => None,
    }
}

/// Tunnel payload since this process started, from this device's side:
/// `tx` went to the peer, `rx` came from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeerTraffic {
    pub peer: String,
    pub tx: u64,
    pub rx: u64,
}

#[derive(Debug, Default)]
pub struct Counter {
    tx: AtomicU64,
    rx: AtomicU64,
}

impl Counter {
    /// Bytes sent to the peer that never passed through a [`Counted`] socket.
    pub fn add_tx(&self, n: usize) {
        self.tx.fetch_add(n as u64, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct Inner {
    conns: HashMap<EndpointId, Connection>,
    traffic: HashMap<EndpointId, Arc<Counter>>,
}

/// Live connections and byte counters by peer. Closed connections drop out on
/// the next snapshot, so callers only ever insert.
#[derive(Debug, Clone, Default)]
pub struct PathBook(Arc<Mutex<Inner>>);

impl PathBook {
    pub fn insert(&self, conn: &Connection) {
        self.lock().conns.insert(conn.remote_id(), conn.clone());
    }

    /// Counter for tunnels with `peer`; wrap the local TCP side with
    /// [`Counted`] to feed it.
    pub fn counter(&self, peer: EndpointId) -> Arc<Counter> {
        self.lock().traffic.entry(peer).or_default().clone()
    }

    pub fn snapshot(&self) -> Vec<PeerPath> {
        let mut inner = self.lock();
        inner.conns.retain(|_, conn| conn.close_reason().is_none());
        let mut out: Vec<PeerPath> = inner
            .conns
            .iter()
            .filter_map(|(peer, conn)| {
                let paths = conn.paths();
                let path = paths.iter().find(|p| p.is_selected())?;
                Some(PeerPath {
                    peer: peer.to_string(),
                    kind: if path.is_relay() { "relay" } else { "direct" },
                    rtt_ms: path.rtt().as_millis() as u64,
                    addr: path_addr(path.remote_addr()),
                })
            })
            .collect();
        out.sort_by(|a, b| a.peer.cmp(&b.peer));
        out
    }

    pub fn traffic(&self) -> Vec<PeerTraffic> {
        let mut out: Vec<PeerTraffic> = self
            .lock()
            .traffic
            .iter()
            .map(|(peer, c)| PeerTraffic {
                peer: peer.to_string(),
                tx: c.tx.load(Ordering::Relaxed),
                rx: c.rx.load(Ordering::Relaxed),
            })
            .filter(|t| t.tx > 0 || t.rx > 0)
            .collect();
        out.sort_by(|a, b| a.peer.cmp(&b.peer));
        out
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0.lock().expect("path book poisoned")
    }
}

/// The local end of a tunnel (the TCP socket), counting as it goes: bytes
/// read from it head to the peer (`tx`), bytes written to it came from the
/// peer (`rx`).
pub struct Counted<T> {
    inner: T,
    counter: Arc<Counter>,
}

impl<T> Counted<T> {
    pub fn new(inner: T, counter: Arc<Counter>) -> Self {
        Self { inner, counter }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Counted<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        let n = (buf.filled().len() - before) as u64;
        self.counter.tx.fetch_add(n, Ordering::Relaxed);
        res
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Counted<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &res {
            self.counter.rx.fetch_add(*n as u64, Ordering::Relaxed);
        }
        res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn counts_both_directions() {
        let book = PathBook::default();
        let peer = SecretKey::generate().public();
        let (a, mut b) = tokio::io::duplex(64);
        let mut counted = Counted::new(a, book.counter(peer));

        counted.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        b.read_exact(&mut buf).await.unwrap();
        b.write_all(b"hi!").await.unwrap();
        let mut buf = [0u8; 3];
        counted.read_exact(&mut buf).await.unwrap();

        let t = book.traffic();
        assert_eq!(t.len(), 1);
        assert_eq!((t[0].tx, t[0].rx), (3, 5));
    }

    #[test]
    fn direct_addr_is_socket_addr() {
        let remote = TransportAddr::Ip("103.59.160.195:46324".parse().unwrap());
        assert_eq!(path_addr(&remote).as_deref(), Some("103.59.160.195:46324"));
    }

    #[test]
    fn relay_addr_is_host_only() {
        let url = "https://use1-1.relay.iroh.network".parse().unwrap();
        let remote = TransportAddr::Relay(url);
        assert_eq!(
            path_addr(&remote).as_deref(),
            Some("use1-1.relay.iroh.network")
        );
    }

    #[test]
    fn addr_is_omitted_when_none() {
        let p = PeerPath {
            peer: "p".into(),
            kind: "direct",
            rtt_ms: 0,
            addr: None,
        };
        let v = serde_json::to_value(&p).unwrap();
        assert!(!v.as_object().unwrap().contains_key("addr"));
    }
}
