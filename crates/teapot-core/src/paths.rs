//! Which path (direct or relay) each live iroh connection is using, for the
//! presence report.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use iroh::{EndpointId, endpoint::Connection};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeerPath {
    pub peer: String,
    pub kind: &'static str,
    pub rtt_ms: u64,
}

/// Live connections by peer. Closed connections drop out on the next snapshot,
/// so callers only ever insert.
#[derive(Debug, Clone, Default)]
pub struct PathBook(Arc<Mutex<HashMap<EndpointId, Connection>>>);

impl PathBook {
    pub fn insert(&self, conn: &Connection) {
        self.0
            .lock()
            .expect("path book poisoned")
            .insert(conn.remote_id(), conn.clone());
    }

    pub fn snapshot(&self) -> Vec<PeerPath> {
        let mut map = self.0.lock().expect("path book poisoned");
        map.retain(|_, conn| conn.close_reason().is_none());
        let mut out: Vec<PeerPath> = map
            .iter()
            .filter_map(|(peer, conn)| {
                let paths = conn.paths();
                let path = paths.iter().find(|p| p.is_selected())?;
                Some(PeerPath {
                    peer: peer.to_string(),
                    kind: if path.is_relay() { "relay" } else { "direct" },
                    rtt_ms: path.rtt().as_millis() as u64,
                })
            })
            .collect();
        out.sort_by(|a, b| a.peer.cmp(&b.peer));
        out
    }
}
