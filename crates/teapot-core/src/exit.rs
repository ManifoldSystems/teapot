//! Exit node (B): accepts proxy streams and dials the internet on the
//! client's behalf, so targets see this machine's IP.

use std::{collections::HashSet, sync::Arc};

use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use teapot_proto::{ConnectRequest, ConnectResponse, read_line};
use tokio::{io::AsyncWriteExt, sync::watch};
use tracing::{debug, info, warn};

use crate::{net, paths::PathBook};

#[derive(Debug, Clone)]
pub enum Allow {
    /// Any endpoint may use this exit node. Only for local experiments.
    Any,
    Only(HashSet<EndpointId>),
    /// Kept current by control; clients dropped from it are disconnected.
    Live(watch::Receiver<HashSet<EndpointId>>),
}

#[derive(Debug, Clone)]
pub struct ExitPolicy {
    pub allow: Allow,
    pub allow_lan: bool,
}

impl ExitPolicy {
    fn permits(&self, peer: &EndpointId) -> bool {
        match &self.allow {
            Allow::Any => true,
            Allow::Only(ids) => ids.contains(peer),
            Allow::Live(rx) => rx.borrow().contains(peer),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExitNode {
    policy: Arc<ExitPolicy>,
    paths: PathBook,
}

impl ExitNode {
    pub fn new(policy: ExitPolicy, paths: PathBook) -> Self {
        Self {
            policy: Arc::new(policy),
            paths,
        }
    }
}

impl ProtocolHandler for ExitNode {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let peer = conn.remote_id();
        if !self.policy.permits(&peer) {
            warn!(%peer, "rejected: not in allowlist");
            conn.close(1u32.into(), b"not allowed");
            return Ok(());
        }
        info!(%peer, "client connected");
        self.paths.insert(&conn);

        let mut live = match &self.policy.allow {
            Allow::Live(rx) => Some(rx.clone()),
            _ => None,
        };

        // One long-lived connection per client; every proxied TCP connection
        // arrives as its own bi-stream.
        loop {
            let accepted = tokio::select! {
                res = conn.accept_bi() => res,
                Some(()) = allowlist_changed(&mut live) => {
                    if !self.policy.permits(&peer) {
                        warn!(%peer, "removed from allowlist; disconnecting");
                        conn.close(1u32.into(), b"not allowed");
                        return Ok(());
                    }
                    continue;
                }
            };
            let (send, recv) = match accepted {
                Ok(streams) => streams,
                Err(e) => {
                    info!(%peer, "client disconnected: {e}");
                    return Ok(());
                }
            };
            let policy = self.policy.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_stream(send, recv, &policy).await {
                    debug!(%peer, "stream ended: {e:#}");
                }
            });
        }
    }
}

/// Resolves when a live allowlist changes; never resolves for static ones.
async fn allowlist_changed(live: &mut Option<watch::Receiver<HashSet<EndpointId>>>) -> Option<()> {
    match live {
        Some(rx) => rx.changed().await.ok(),
        None => std::future::pending().await,
    }
}

async fn handle_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    policy: &ExitPolicy,
) -> anyhow::Result<()> {
    let req = ConnectRequest::parse(&read_line(&mut recv).await?)?;

    let mut tcp = match net::dial(&req.target, policy.allow_lan).await {
        Ok(tcp) => tcp,
        Err(e) => {
            warn!(target = %req.target, "dial refused: {e:#}");
            send.write_all(&ConnectResponse::Err(format!("{e:#}")).encode())
                .await?;
            send.finish()?;
            return Err(e);
        }
    };
    send.write_all(&ConnectResponse::Ok.encode()).await?;
    debug!(target = %req.target, "tunnel open");

    let mut tunnel = tokio::io::join(recv, send);
    let (up, down) = tokio::io::copy_bidirectional(&mut tunnel, &mut tcp).await?;
    tunnel.shutdown().await.ok();
    debug!(target = %req.target, up, down, "tunnel closed");
    Ok(())
}
