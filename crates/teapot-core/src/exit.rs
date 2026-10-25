//! Exit node (B): accepts proxy streams and dials the internet on the
//! client's behalf, so targets see this machine's IP.

use std::{collections::HashSet, sync::Arc};

use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use teapot_proto::{ConnectRequest, ConnectResponse, read_line};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

use crate::net;

#[derive(Debug, Clone)]
pub enum Allow {
    /// Any endpoint may use this exit node. Only for local experiments.
    Any,
    Only(HashSet<EndpointId>),
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
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExitNode {
    policy: Arc<ExitPolicy>,
}

impl ExitNode {
    pub fn new(policy: ExitPolicy) -> Self {
        Self {
            policy: Arc::new(policy),
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

        // One long-lived connection per client; every proxied TCP connection
        // arrives as its own bi-stream.
        loop {
            let (send, recv) = match conn.accept_bi().await {
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
