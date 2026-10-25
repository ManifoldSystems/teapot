//! Long-running agent tasks that keep a device in step with control: joining,
//! the exit node's allowlist, and the proxy's choice of exit node.

use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use anyhow::Context;
use iroh::{EndpointId, SecretKey};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::{
    control::{self, ControlClient, DeviceInfo, Peer},
    proxy::ProxyClient,
};

/// Fallback refresh when no `peers_changed` arrives.
const REFRESH: Duration = Duration::from_secs(30);

/// Logs in, registers name/platform/exit capability, and points at the
/// approval page if the device is still pending.
pub async fn join(
    base: &str,
    key: SecretKey,
    info: DeviceInfo,
) -> anyhow::Result<Arc<ControlClient>> {
    let control = Arc::new(ControlClient::new(base, key)?);
    let status = control
        .login()
        .await
        .with_context(|| format!("logging in to {}", control.base()))?;
    let name = info.name.clone().unwrap_or_default();
    control.update_me(&info).await?;
    if status == "approved" {
        info!(%name, "logged in to control");
    } else {
        warn!(
            "{name} ({}) is waiting for approval; run `teapot login` or open {}",
            &control.node_id()[..8],
            control.approve_url()
        );
    }
    Ok(control)
}

/// Keeps an exit node's allowlist equal to control's. Refetches on every
/// `peers_changed` and every 30 s.
pub async fn sync_allowlist(control: Arc<ControlClient>, tx: watch::Sender<HashSet<EndpointId>>) {
    let mut changed = control.subscribe();
    let mut enabled: Option<bool> = None;
    loop {
        match control.allowlist().await {
            Ok(list) => {
                if enabled != Some(list.exit_enabled) {
                    if list.exit_enabled {
                        info!("exit is enabled in the dashboard");
                    } else {
                        info!(
                            "exit is off for this device; turn it on in the dashboard to share it"
                        );
                    }
                    enabled = Some(list.exit_enabled);
                }
                let set: HashSet<EndpointId> = list
                    .allow
                    .iter()
                    .filter_map(|id| EndpointId::from_str(id).ok())
                    .collect();
                tx.send_if_modified(|current| {
                    if *current == set {
                        return false;
                    }
                    info!(clients = set.len(), "allowlist updated");
                    *current = set;
                    true
                });
            }
            Err(e) if control::is_pending(&e) => {}
            Err(e) => warn!("allowlist: {e:#}"),
        }
        wait_for_change(&mut changed).await;
    }
}

/// Keeps the proxy pointed at a usable exit node: the preferred one when it
/// is online, otherwise the current one while it stays online, otherwise the
/// first online exit. Runs until the process ends.
pub async fn follow_exits(
    control: Arc<ControlClient>,
    client: Arc<ProxyClient>,
    preferred: Option<String>,
) {
    let mut changed = control.subscribe();
    let mut last_problem: Option<String> = None;
    loop {
        let decision = match control.peers().await {
            Ok(peers) => {
                let current = client.exit().await.map(|id| id.to_string());
                pick_exit(&peers, preferred.as_deref(), current.as_deref())
                    .map(|p| (p.node_id.clone(), p.label()))
            }
            Err(e) if control::is_pending(&e) => {
                Err("this device is waiting for approval".to_string())
            }
            Err(e) => {
                // Keep the current exit; control being unreachable says
                // nothing about the exit node itself.
                warn!("peers: {e:#}");
                wait_for_change(&mut changed).await;
                continue;
            }
        };
        match decision {
            Ok((id, label)) => match EndpointId::from_str(&id) {
                Ok(id) => {
                    if client.set_exit(Some(id)).await {
                        info!(exit = %label, "using exit node");
                    }
                    last_problem = None;
                }
                Err(e) => warn!("bad endpoint id from control: {e}"),
            },
            Err(problem) => {
                let dropped = client.set_exit(None).await;
                if dropped || last_problem.as_ref() != Some(&problem) {
                    warn!("no exit node: {problem}");
                }
                last_problem = Some(problem);
            }
        }
        wait_for_change(&mut changed).await;
    }
}

async fn wait_for_change(changed: &mut watch::Receiver<u64>) {
    tokio::select! {
        _ = changed.changed() => {}
        _ = tokio::time::sleep(REFRESH) => {}
    }
}

/// Chooses an exit node. Order: `preferred` if usable, then `current` if
/// still usable (no flapping between equals), then the first usable one.
/// Usable = exit turned on by the owner and online. The error says why
/// nothing qualifies.
pub fn pick_exit<'a>(
    peers: &'a [Peer],
    preferred: Option<&str>,
    current: Option<&str>,
) -> Result<&'a Peer, String> {
    let usable = |p: &&Peer| p.exit && p.online;

    if let Some(wanted) = preferred {
        match peers.iter().find(|p| matches(p, wanted)) {
            Some(p) if usable(&p) => return Ok(p),
            Some(p) if !p.exit => {
                return Err(format!(
                    "{} is not an enabled exit node; turn exit on in the dashboard",
                    p.label()
                ));
            }
            Some(_) => {} // offline: fall back below, come back when it returns
            None => return Err(format!("no approved device matches {wanted:?}")),
        }
    }
    if let Some(id) = current
        && let Some(p) = peers.iter().find(|p| p.node_id == id).filter(usable)
    {
        return Ok(p);
    }
    if let Some(p) = peers.iter().find(usable) {
        return Ok(p);
    }

    let exits: Vec<String> = peers.iter().filter(|p| p.exit).map(|p| p.label()).collect();
    Err(if exits.is_empty() {
        "no exit nodes; turn exit on for a device in the dashboard".into()
    } else {
        format!("no exit node is online (offline: {})", exits.join(", "))
    })
}

/// Name (case-insensitive), full id, or an id prefix of at least 8 chars.
fn matches(p: &Peer, wanted: &str) -> bool {
    p.node_id == wanted
        || p.name
            .as_deref()
            .is_some_and(|n| n.eq_ignore_ascii_case(wanted))
        || (wanted.len() >= 8 && p.node_id.starts_with(wanted))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(name: &str, online: bool, exit: bool) -> Peer {
        Peer {
            node_id: SecretKey::generate().public().to_string(),
            name: Some(name.into()),
            platform: None,
            online,
            exit,
        }
    }

    #[test]
    fn first_online_exit() {
        let peers = [
            peer("laptop", true, false),
            peer("vps", false, true),
            peer("raspi", true, true),
        ];
        assert_eq!(pick_exit(&peers, None, None).unwrap().label(), "raspi");
    }

    #[test]
    fn by_name_or_prefix() {
        let peers = [peer("raspi", true, true), peer("vps", true, true)];
        assert_eq!(pick_exit(&peers, Some("VPS"), None).unwrap().label(), "vps");
        let prefix = peers[0].node_id[..10].to_string();
        assert_eq!(
            pick_exit(&peers, Some(&prefix), None).unwrap().label(),
            "raspi"
        );
    }

    #[test]
    fn sticks_to_current_while_online() {
        let peers = [peer("raspi", true, true), peer("vps", true, true)];
        let current = peers[1].node_id.clone();
        assert_eq!(
            pick_exit(&peers, None, Some(&current)).unwrap().label(),
            "vps"
        );
    }

    #[test]
    fn fails_over_and_returns_to_preferred() {
        let mut peers = [peer("raspi", false, true), peer("vps", true, true)];
        let raspi = peers[0].node_id.clone();
        // preferred offline -> fall back
        assert_eq!(
            pick_exit(&peers, Some("raspi"), Some(&raspi))
                .unwrap()
                .label(),
            "vps"
        );
        // preferred back -> return even though current (vps) is fine
        peers[0].online = true;
        let vps = peers[1].node_id.clone();
        assert_eq!(
            pick_exit(&peers, Some("raspi"), Some(&vps))
                .unwrap()
                .label(),
            "raspi"
        );
    }

    #[test]
    fn explains_why_not() {
        let peers = [peer("laptop", true, false), peer("vps", false, true)];
        let err = pick_exit(&peers, Some("laptop"), None).unwrap_err();
        assert!(err.contains("not an enabled exit node"), "{err}");
        let err = pick_exit(&peers, None, None).unwrap_err();
        assert!(err.contains("offline: vps"), "{err}");
        let err = pick_exit(&peers[..1], None, None).unwrap_err();
        assert!(err.contains("no exit nodes"), "{err}");
        let err = pick_exit(&peers, Some("nope"), None).unwrap_err();
        assert!(err.contains("no approved device"), "{err}");
    }
}
