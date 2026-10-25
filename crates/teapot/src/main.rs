use std::{
    collections::HashSet, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc, time::Duration,
};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use iroh::{EndpointId, SecretKey, protocol::Router};
use teapot_core::{
    control::{self, ControlClient, DeviceInfo, Peer},
    exit::{Allow, ExitNode, ExitPolicy},
    key,
    paths::PathBook,
    proxy::{self, ProxyClient},
};
use teapot_proto::ALPN_PROXY;
use tokio::sync::watch;
use tracing::{info, warn};

/// Each buyer's clone points this at their own deployment.
const DEFAULT_CONTROL: &str = "https://teapot-control.example.workers.dev";

#[derive(Parser)]
#[command(version, about = "Personal mesh on iroh")]
struct Cli {
    /// Secret key file; the endpoint id is derived from it.
    #[arg(long, global = true, env = "TEAPOT_KEY")]
    key: Option<PathBuf>,

    /// Control plane URL.
    #[arg(long, global = true, env = "TEAPOT_CONTROL", default_value = DEFAULT_CONTROL)]
    control: String,

    /// Device name shown in the dashboard. Default: hostname.
    #[arg(long, global = true, env = "TEAPOT_NAME")]
    name: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print this device's endpoint id.
    Id,
    /// Add this device to your mesh: opens the approval page in a browser
    /// and waits until you confirm.
    Login {
        /// Print the link instead of opening a browser.
        #[arg(long)]
        no_browser: bool,
    },
    /// Log in to control and show this device and its peers.
    Status,
    /// Act as an exit node: dial the internet for allowed clients.
    ///
    /// The allowlist comes from control unless --allow or --allow-any is given.
    Exit {
        /// Endpoint id allowed to use this exit node, without control. Repeatable.
        #[arg(long = "allow", value_name = "ENDPOINT_ID")]
        allow: Vec<EndpointId>,
        /// Allow every endpoint, without control. Only for local experiments.
        #[arg(long, conflicts_with = "allow")]
        allow_any: bool,
        /// Let clients reach this machine's LAN and loopback.
        #[arg(long)]
        allow_lan: bool,
    },
    /// Run a local HTTP proxy that exits through another device.
    Proxy {
        /// Exit node: name, endpoint id, or id prefix. Default: the first
        /// online exit node from control.
        exit: Option<String>,
        #[arg(long, default_value = "127.0.0.1:1080")]
        listen: SocketAddr,
        /// Skip control; EXIT must then be a full endpoint id.
        #[arg(long)]
        no_control: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "teapot=info,teapot_core=info".into()),
        )
        .init();
    // iroh is built on ring; give reqwest and tungstenite the same provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    let key_path = match &cli.key {
        Some(path) => path.clone(),
        None => dirs::config_dir()
            .context("no config directory on this platform; pass --key")?
            .join("teapot")
            .join("secret.key"),
    };
    let secret_key = key::load_or_create(&key_path)?;

    match cli.cmd {
        Cmd::Id => println!("{}", secret_key.public()),
        Cmd::Login { no_browser } => login(&cli, secret_key, no_browser).await?,
        Cmd::Status => status(&cli.control, secret_key).await?,
        Cmd::Exit {
            ref allow,
            allow_any,
            allow_lan,
        } => {
            let paths = PathBook::default();
            let allow = if allow_any {
                Allow::Any
            } else if !allow.is_empty() {
                Allow::Only(allow.iter().copied().collect::<HashSet<_>>())
            } else {
                let control = join_control(&cli, secret_key.clone(), Some(true)).await?;
                tokio::spawn(control.clone().run_presence(paths.clone()));
                let (tx, rx) = watch::channel(HashSet::new());
                tokio::spawn(sync_allowlist(control, tx));
                Allow::Live(rx)
            };
            let endpoint = teapot_core::bind(secret_key).await?;
            let router = Router::builder(endpoint)
                .accept(
                    ALPN_PROXY,
                    ExitNode::new(ExitPolicy { allow, allow_lan }, paths),
                )
                .spawn();
            router.endpoint().online().await;
            info!(id = %router.endpoint().id(), allow_lan, "exit node ready");
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        Cmd::Proxy {
            ref exit,
            listen,
            no_control,
        } => {
            let paths = PathBook::default();
            let exit_id = if no_control {
                let Some(exit) = exit else {
                    bail!("--no-control needs the exit node's endpoint id");
                };
                EndpointId::from_str(exit).context("EXIT must be a full endpoint id")?
            } else {
                let control = join_control(&cli, secret_key.clone(), None).await?;
                tokio::spawn(control.clone().run_presence(paths.clone()));
                let peers = wait_for_peers(&control).await?;
                let (id, label) = choose_exit(&peers, exit.as_deref())?;
                info!(exit = %label, "using exit node");
                id
            };

            let endpoint = teapot_core::bind(secret_key).await?;
            info!(id = %endpoint.id(), "client ready");
            let client = Arc::new(ProxyClient::new(endpoint.clone(), exit_id, paths));
            tokio::select! {
                res = proxy::serve(listen, client) => res?,
                _ = tokio::signal::ctrl_c() => {}
            }
            endpoint.close().await;
        }
    }
    Ok(())
}

fn device_name(cli: &Cli) -> String {
    cli.name.clone().unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .map(|h| h.trim_end_matches(".local").to_string())
            .unwrap_or_else(|| "teapot".into())
    })
}

/// Logs in, registers name/platform, and says so if approval is pending.
async fn join_control(
    cli: &Cli,
    key: SecretKey,
    can_exit: Option<bool>,
) -> anyhow::Result<Arc<ControlClient>> {
    let control = Arc::new(ControlClient::new(&cli.control, key)?);
    let status = control
        .login()
        .await
        .with_context(|| format!("logging in to {}", control.base()))?;
    let name = device_name(cli);
    control
        .update_me(&DeviceInfo {
            name: Some(name.clone()),
            platform: Some(std::env::consts::OS.into()),
            can_exit,
        })
        .await?;
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

/// Retries `/v1/peers` until the device is approved.
async fn wait_for_peers(control: &ControlClient) -> anyhow::Result<Vec<Peer>> {
    let mut changed = control.subscribe();
    loop {
        match control.peers().await {
            Ok(peers) => return Ok(peers),
            Err(e) if control::is_pending(&e) => {}
            Err(e) => return Err(e),
        }
        tokio::select! {
            _ = changed.changed() => {}
            _ = tokio::time::sleep(Duration::from_secs(30)) => {}
        }
    }
}

fn choose_exit(peers: &[Peer], wanted: Option<&str>) -> anyhow::Result<(EndpointId, String)> {
    let pick = match wanted {
        Some(w) => {
            let found = peers.iter().find(|p| {
                p.node_id == w
                    || p.name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(w))
                    || (w.len() >= 8 && p.node_id.starts_with(w))
            });
            let Some(p) = found else {
                bail!("no approved device matches {w:?}; see `teapot status`");
            };
            if !p.exit {
                bail!(
                    "{} is not an enabled exit node; turn exit on in the dashboard",
                    p.label()
                );
            }
            if !p.online {
                warn!("{} is offline right now; will keep trying", p.label());
            }
            p
        }
        None => {
            let exits: Vec<&Peer> = peers.iter().filter(|p| p.exit).collect();
            let Some(p) = exits.iter().find(|p| p.online) else {
                if exits.is_empty() {
                    bail!(
                        "no exit nodes; run `teapot exit` on a device and enable it in the dashboard"
                    );
                }
                let names: Vec<String> = exits.iter().map(|p| p.label()).collect();
                bail!("no exit node is online (offline: {})", names.join(", "));
            };
            let others: Vec<String> = exits
                .iter()
                .filter(|o| o.online && o.node_id != p.node_id)
                .map(|o| o.label())
                .collect();
            if !others.is_empty() {
                info!(
                    "also online: {} (`teapot proxy <name>` to choose)",
                    others.join(", ")
                );
            }
            *p
        }
    };
    Ok((EndpointId::from_str(&pick.node_id)?, pick.label()))
}

/// Keeps the exit node's allowlist equal to control's, refetching whenever
/// control broadcasts a change (and every minute as a fallback).
async fn sync_allowlist(control: Arc<ControlClient>, tx: watch::Sender<HashSet<EndpointId>>) {
    let mut changed = control.subscribe();
    let mut enabled: Option<bool> = None;
    loop {
        match control.allowlist().await {
            Ok(list) => {
                if enabled != Some(list.exit_enabled) {
                    if list.exit_enabled {
                        info!("exit is enabled in the dashboard");
                    } else {
                        warn!("exit is off for this device; turn it on in the dashboard");
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
        tokio::select! {
            _ = changed.changed() => {}
            _ = tokio::time::sleep(Duration::from_secs(60)) => {}
        }
    }
}

async fn login(cli: &Cli, key: SecretKey, no_browser: bool) -> anyhow::Result<()> {
    let control = ControlClient::new(&cli.control, key)?;
    let status = control
        .login()
        .await
        .with_context(|| format!("logging in to {}", control.base()))?;
    let name = device_name(cli);
    control
        .update_me(&DeviceInfo {
            name: Some(name.clone()),
            platform: Some(std::env::consts::OS.into()),
            can_exit: None,
        })
        .await?;
    if status == "approved" {
        println!("{name} is already in your mesh.");
        return Ok(());
    }

    let url = control.approve_url();
    println!("To add {name} to your mesh, open:\n\n  {url}\n");
    println!(
        "Check that the page shows code {}.\n",
        &control.node_id()[..8]
    );
    if !no_browser && let Err(e) = open_browser(&url) {
        println!("(could not open a browser: {e}; open the link yourself)\n");
    }

    println!("Waiting for approval… (Ctrl+C to stop)");
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        match control.me().await {
            Ok(me) if me.status == "approved" => break,
            Ok(_) => {}
            Err(e) => warn!("{e:#}"),
        }
    }
    println!("✓ {name} added. Run `teapot exit` or `teapot proxy` on it.");
    Ok(())
}

fn open_browser(url: &str) -> std::io::Result<()> {
    let mut cmd = if cfg!(target_os = "macos") {
        std::process::Command::new("open")
    } else if cfg!(windows) {
        let mut c = std::process::Command::new("cmd");
        c.args(["/C", "start", ""]);
        c
    } else {
        std::process::Command::new("xdg-open")
    };
    let status = cmd.arg(url).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!("exit status {status}")))
    }
}

async fn status(base: &str, key: SecretKey) -> anyhow::Result<()> {
    let control = ControlClient::new(base, key)?;
    control
        .login()
        .await
        .with_context(|| format!("logging in to {base}"))?;
    let me = control.me().await?;
    println!("control   {base}");
    println!(
        "device    {} ({})",
        me.name.as_deref().unwrap_or("unnamed"),
        me.node_id
    );
    println!("status    {}", me.status);
    println!(
        "exit      {}",
        match (me.can_exit, me.exit_enabled) {
            (true, true) => "on",
            (true, false) => "capable, off in dashboard",
            _ => "not running as exit node",
        }
    );
    println!("public ip {}", me.last_ip.as_deref().unwrap_or("-"));

    match control.peers().await {
        Ok(peers) if peers.is_empty() => println!("\nno other approved devices"),
        Ok(peers) => {
            println!(
                "\n{:<20} {:<10} {:<8} {:<5} id",
                "peer", "platform", "online", "exit"
            );
            for p in peers {
                println!(
                    "{:<20} {:<10} {:<8} {:<5} {}",
                    p.label(),
                    p.platform.as_deref().unwrap_or("-"),
                    if p.online { "yes" } else { "no" },
                    if p.exit { "yes" } else { "-" },
                    &p.node_id[..16]
                );
            }
        }
        Err(e) if control::is_pending(&e) => {
            println!("\nwaiting for approval; approve this device at {base}")
        }
        Err(e) => return Err(e),
    }
    Ok(())
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
    fn picks_first_online_exit() {
        let peers = [
            peer("laptop", true, false),
            peer("vps", false, true),
            peer("raspi", true, true),
        ];
        assert_eq!(choose_exit(&peers, None).unwrap().1, "raspi");
    }

    #[test]
    fn by_name_or_prefix() {
        let peers = [peer("raspi", true, true), peer("vps", true, true)];
        assert_eq!(choose_exit(&peers, Some("VPS")).unwrap().1, "vps");
        let prefix = &peers[0].node_id[..10];
        assert_eq!(choose_exit(&peers, Some(prefix)).unwrap().1, "raspi");
    }

    #[test]
    fn explains_why_not() {
        let peers = [peer("laptop", true, false), peer("vps", false, true)];
        let err = choose_exit(&peers, Some("laptop")).unwrap_err().to_string();
        assert!(err.contains("not an enabled exit node"), "{err}");
        let err = choose_exit(&peers, None).unwrap_err().to_string();
        assert!(err.contains("offline: vps"), "{err}");
        let err = choose_exit(&peers[..1], None).unwrap_err().to_string();
        assert!(err.contains("no exit nodes"), "{err}");
    }
}
