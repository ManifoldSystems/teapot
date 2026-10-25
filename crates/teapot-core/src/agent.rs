//! Long-running agent tasks that keep a device in step with control: joining,
//! the exit node's allowlist, and the proxy's choice of exit node.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use iroh::{EndpointId, SecretKey};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::{
    control::{self, ControlClient, DeviceInfo, Peer},
    proxy::ProxyClient,
};

/// Feeds control this device's public IPv4/IPv6, so the dashboard shows both
/// families, not only the one the presence socket happens to use. The
/// endpoint's own address list already holds what its QAD probes learned;
/// LAN, loopback, CGNAT/Tailscale and ULA addresses are dropped.
pub async fn report_public_ips(control: Arc<ControlClient>, endpoint: iroh::Endpoint) {
    loop {
        let addr = endpoint.addr();
        let public = || {
            addr.ip_addrs()
                .map(|a| a.ip())
                .filter(|ip| !crate::net::is_local(*ip))
        };
        control.set_public_ips(control::PublicIps {
            v4: public().find_map(|ip| match ip {
                std::net::IpAddr::V4(v4) => Some(v4),
                _ => None,
            }),
            v6: public().find_map(|ip| match ip {
                std::net::IpAddr::V6(v6) => Some(v6),
                _ => None,
            }),
        });
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

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
///
/// Each new list is written to `cache`, so a restart while control is
/// unreachable starts from the last known list instead of refusing everyone
/// (see [`load_allowlist`]).
pub async fn sync_allowlist(
    control: Arc<ControlClient>,
    tx: watch::Sender<HashSet<EndpointId>>,
    cache: Option<PathBuf>,
) {
    let mut changed = control.subscribe();
    let mut enabled: Option<bool> = None;
    let mut cached_once = false;
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
                let changed_now = tx.send_if_modified(|current| {
                    if *current == set {
                        return false;
                    }
                    info!(clients = set.len(), "allowlist updated");
                    *current = set.clone();
                    true
                });
                if (changed_now || !cached_once)
                    && let Some(path) = &cache
                {
                    match save_allowlist(path, &set) {
                        Ok(()) => cached_once = true,
                        Err(e) => warn!("caching allowlist: {e:#}"),
                    }
                }
            }
            Err(e) if control::is_pending(&e) => {}
            Err(e) => warn!("allowlist: {e:#}"),
        }
        wait_for_change(&mut changed).await;
    }
}

/// The allowlist cached by [`sync_allowlist`], if any.
pub fn load_allowlist(path: &Path) -> Option<HashSet<EndpointId>> {
    let text = std::fs::read_to_string(path).ok()?;
    let ids: Vec<String> = serde_json::from_str(&text).ok()?;
    Some(
        ids.iter()
            .filter_map(|id| EndpointId::from_str(id).ok())
            .collect(),
    )
}

fn save_allowlist(path: &Path, set: &HashSet<EndpointId>) -> anyhow::Result<()> {
    let mut ids: Vec<String> = set.iter().map(|id| id.to_string()).collect();
    ids.sort();
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec(&ids)?)?;
    std::fs::rename(&tmp, path)?; // atomic: never a half-written list
    Ok(())
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
                // Per-request exits (proxy username) resolve against this list.
                client.set_peers(peers.clone());
                let current = client.exit().await.map(|id| id.to_string());
                let my_ipv4 = control.public_ips().v4.map(|ip| ip.to_string());
                pick_exit(
                    &peers,
                    preferred.as_deref(),
                    current.as_deref(),
                    my_ipv4.as_deref(),
                )
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
    my_ipv4: Option<&str>,
) -> Result<&'a Peer, String> {
    // An exit on this device's own network would leave with this device's own
    // IP — pointless, so it counts as unusable (e.g. a laptop whose hotspot was
    // switched off and joined the office Wi-Fi).
    let same_ip = |p: &Peer| my_ipv4.is_some() && p.public_ipv4.as_deref() == my_ipv4;
    let usable = |p: &&Peer| p.exit && p.online && !same_ip(p);

    if let Some(wanted) = preferred {
        match peers.iter().find(|p| p.matches(wanted)) {
            Some(p) if usable(&p) => return Ok(p),
            Some(p) if !p.exit => {
                return Err(format!(
                    "{} is not an enabled exit node; turn exit on in the dashboard",
                    p.label()
                ));
            }
            Some(_) => {} // offline or same IP: fall back below, come back later
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

    let exits: Vec<&Peer> = peers.iter().filter(|p| p.exit).collect();
    let online_same: Vec<String> = exits
        .iter()
        .filter(|p| p.online && same_ip(p))
        .map(|p| p.label())
        .collect();
    Err(if exits.is_empty() {
        "no exit nodes; turn exit on for a device in the dashboard".into()
    } else if !online_same.is_empty() {
        format!(
            "every online exit leaves with this device's own IP {} ({})",
            my_ipv4.unwrap_or("?"),
            online_same.join(", ")
        )
    } else {
        let names: Vec<String> = exits.iter().map(|p| p.label()).collect();
        format!("no exit node is online (offline: {})", names.join(", "))
    })
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
            public_ipv4: None,
        }
    }

    fn at(mut p: Peer, ip: &str) -> Peer {
        p.public_ipv4 = Some(ip.into());
        p
    }

    #[test]
    fn allowlist_cache_roundtrip() {
        let dir = std::env::temp_dir().join(format!("teapot-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("allowlist.json");
        let set: HashSet<EndpointId> = (0..3).map(|_| SecretKey::generate().public()).collect();
        save_allowlist(&path, &set).unwrap();
        assert_eq!(load_allowlist(&path), Some(set));
        assert_eq!(load_allowlist(&dir.join("missing.json")), None);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn first_online_exit() {
        let peers = [
            peer("laptop", true, false),
            peer("vps", false, true),
            peer("raspi", true, true),
        ];
        assert_eq!(
            pick_exit(&peers, None, None, None).unwrap().label(),
            "raspi"
        );
    }

    #[test]
    fn by_name_or_prefix() {
        let peers = [peer("raspi", true, true), peer("vps", true, true)];
        assert_eq!(
            pick_exit(&peers, Some("VPS"), None, None).unwrap().label(),
            "vps"
        );
        let prefix = peers[0].node_id[..10].to_string();
        assert_eq!(
            pick_exit(&peers, Some(&prefix), None, None)
                .unwrap()
                .label(),
            "raspi"
        );
    }

    #[test]
    fn sticks_to_current_while_online() {
        let peers = [peer("raspi", true, true), peer("vps", true, true)];
        let current = peers[1].node_id.clone();
        assert_eq!(
            pick_exit(&peers, None, Some(&current), None)
                .unwrap()
                .label(),
            "vps"
        );
    }

    #[test]
    fn fails_over_and_returns_to_preferred() {
        let mut peers = [peer("raspi", false, true), peer("vps", true, true)];
        let raspi = peers[0].node_id.clone();
        // preferred offline -> fall back
        assert_eq!(
            pick_exit(&peers, Some("raspi"), Some(&raspi), None)
                .unwrap()
                .label(),
            "vps"
        );
        // preferred back -> return even though current (vps) is fine
        peers[0].online = true;
        let vps = peers[1].node_id.clone();
        assert_eq!(
            pick_exit(&peers, Some("raspi"), Some(&vps), None)
                .unwrap()
                .label(),
            "raspi"
        );
    }

    #[test]
    fn skips_exits_with_own_ip() {
        let office = "160.22.206.210";
        let peers = [
            at(peer("quantumz", true, true), office), // hotspot off, now on office Wi-Fi
            at(peer("algo-apps", true, true), office),
            at(peer("algo-node-db", true, true), "103.3.221.214"),
        ];
        let got = pick_exit(&peers, Some("quantumz"), None, Some(office)).unwrap();
        assert_eq!(got.label(), "algo-node-db");
        // Hotspot back on: quantumz differs again and wins as the preferred one.
        let peers = [
            at(peer("quantumz", true, true), "182.3.37.120"),
            peers[2].clone(),
        ];
        let got = pick_exit(&peers, Some("quantumz"), None, Some(office)).unwrap();
        assert_eq!(got.label(), "quantumz");
    }

    #[test]
    fn unknown_ips_never_exclude() {
        let peers = [peer("quantumz", true, true)];
        assert!(pick_exit(&peers, None, None, Some("160.22.206.210")).is_ok());
        let peers = [at(peer("quantumz", true, true), "160.22.206.210")];
        assert!(pick_exit(&peers, None, None, None).is_ok());
    }

    #[test]
    fn explains_same_ip() {
        let office = "160.22.206.210";
        let peers = [at(peer("algo-apps", true, true), office)];
        let err = pick_exit(&peers, None, None, Some(office)).unwrap_err();
        assert!(err.contains("own IP 160.22.206.210 (algo-apps)"), "{err}");
    }

    #[test]
    fn explains_why_not() {
        let peers = [peer("laptop", true, false), peer("vps", false, true)];
        let err = pick_exit(&peers, Some("laptop"), None, None).unwrap_err();
        assert!(err.contains("not an enabled exit node"), "{err}");
        let err = pick_exit(&peers, None, None, None).unwrap_err();
        assert!(err.contains("offline: vps"), "{err}");
        let err = pick_exit(&peers[..1], None, None, None).unwrap_err();
        assert!(err.contains("no exit nodes"), "{err}");
        let err = pick_exit(&peers, Some("nope"), None, None).unwrap_err();
        assert!(err.contains("no approved device"), "{err}");
    }
}
