mod service;

use std::{
    collections::HashSet, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc, time::Duration,
};

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use iroh::{EndpointId, SecretKey, protocol::Router};
use teapot_core::{
    agent,
    control::{self, ControlClient, DeviceInfo},
    exit::{Allow, ExitNode, ExitPolicy},
    key,
    paths::PathBook,
    proxy::{self, ProxyClient},
};
use teapot_proto::ALPN_PROXY;
use tokio::sync::watch;
use tracing::{info, warn};

/// Each buyer's clone points this at their own deployment.
pub const DEFAULT_CONTROL: &str = "https://teapot-control.sociopath-xyz.workers.dev";

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
    /// Stay online: serve as an exit node when the dashboard allows it, and
    /// run a local proxy that exits through another device.
    Up(UpArgs),
    /// Install or remove `teapot up` as a background service.
    Service {
        #[command(subcommand)]
        cmd: ServiceCmd,
    },
    /// Only serve as an exit node (same as `up --no-proxy`).
    ///
    /// --allow / --allow-any skip control and use a static allowlist.
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
    /// Only run the local proxy (same as `up --no-exit --exit <EXIT>`).
    Proxy {
        /// Preferred exit node: name, endpoint id, or id prefix.
        exit: Option<String>,
        #[arg(long, default_value = "127.0.0.1:1080")]
        listen: SocketAddr,
        /// Skip control; EXIT must then be a full endpoint id.
        #[arg(long)]
        no_control: bool,
    },
}

#[derive(Args, Clone, Debug)]
pub struct UpArgs {
    /// Preferred exit node (name, id, or id prefix). Falls back to another
    /// online exit while it is offline. Default: any online exit.
    #[arg(long)]
    exit: Option<String>,
    /// Local HTTP proxy address.
    #[arg(long, default_value = "127.0.0.1:1080")]
    listen: SocketAddr,
    /// Do not run the local proxy.
    #[arg(long)]
    no_proxy: bool,
    /// Do not offer this device as an exit node.
    #[arg(long, conflicts_with = "no_proxy")]
    no_exit: bool,
    /// Let exit clients reach this machine's LAN and loopback.
    #[arg(long)]
    allow_lan: bool,
}

#[derive(Subcommand)]
enum ServiceCmd {
    /// Write a launchd agent (macOS) or systemd user unit (Linux) that runs
    /// `teapot up` with these options, and start it.
    Install(UpArgs),
    /// Stop and remove the service.
    Uninstall,
    /// Show whether the service is installed and running.
    Status,
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

    // Service management never touches the key or the network.
    if let Cmd::Service { cmd } = &cli.cmd {
        return match cmd {
            ServiceCmd::Install(args) => service::install(
                &service::global_args(&cli.key, &cli.control, &cli.name),
                args,
            ),
            ServiceCmd::Uninstall => service::uninstall(),
            ServiceCmd::Status => service::status(),
        };
    }

    let key_path = match &cli.key {
        Some(path) => path.clone(),
        None => dirs::config_dir()
            .context("no config directory on this platform; pass --key")?
            .join("teapot")
            .join("secret.key"),
    };
    let secret_key = key::load_or_create(&key_path)?;

    match &cli.cmd {
        Cmd::Id => println!("{}", secret_key.public()),
        Cmd::Login { no_browser } => login(&cli, secret_key, *no_browser).await?,
        Cmd::Status => status(&cli.control, secret_key).await?,
        Cmd::Service { .. } => unreachable!("handled above"),
        Cmd::Up(args) => up(&cli, secret_key, args.clone()).await?,
        Cmd::Exit {
            allow,
            allow_any,
            allow_lan,
        } => {
            if *allow_any || !allow.is_empty() {
                let allow = if *allow_any {
                    Allow::Any
                } else {
                    Allow::Only(allow.iter().copied().collect())
                };
                static_exit(secret_key, allow, *allow_lan).await?;
            } else {
                let args = UpArgs {
                    exit: None,
                    listen: "127.0.0.1:1080".parse()?,
                    no_proxy: true,
                    no_exit: false,
                    allow_lan: *allow_lan,
                };
                up(&cli, secret_key, args).await?;
            }
        }
        Cmd::Proxy {
            exit,
            listen,
            no_control,
        } => {
            if *no_control {
                let Some(exit) = exit else {
                    bail!("--no-control needs the exit node's endpoint id");
                };
                let id = EndpointId::from_str(exit).context("EXIT must be a full endpoint id")?;
                static_proxy(secret_key, id, *listen).await?;
            } else {
                let args = UpArgs {
                    exit: exit.clone(),
                    listen: *listen,
                    no_proxy: false,
                    no_exit: true,
                    allow_lan: false,
                };
                up(&cli, secret_key, args).await?;
            }
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

/// One endpoint, one presence socket: exit node and proxy side by side.
async fn up(cli: &Cli, key: SecretKey, args: UpArgs) -> anyhow::Result<()> {
    let offer_exit = !args.no_exit;
    let run_proxy = !args.no_proxy;
    let paths = PathBook::default();

    let control = agent::join(
        &cli.control,
        key.clone(),
        DeviceInfo {
            name: Some(device_name(cli)),
            platform: Some(std::env::consts::OS.into()),
            can_exit: Some(offer_exit),
        },
    )
    .await?;
    tokio::spawn(control.clone().run_presence(paths.clone()));

    let endpoint = teapot_core::bind(key).await?;

    let router = offer_exit.then(|| {
        let (tx, rx) = watch::channel(HashSet::new());
        tokio::spawn(agent::sync_allowlist(control.clone(), tx));
        let policy = ExitPolicy {
            allow: Allow::Live(rx),
            allow_lan: args.allow_lan,
        };
        Router::builder(endpoint.clone())
            .accept(ALPN_PROXY, ExitNode::new(policy, paths.clone()))
            .spawn()
    });

    let proxy_task = if run_proxy {
        let client = Arc::new(ProxyClient::new(endpoint.clone(), None, paths.clone()));
        tokio::spawn(agent::follow_exits(
            control.clone(),
            client.clone(),
            args.exit.clone(),
        ));
        Some(tokio::spawn(proxy::serve(args.listen, client)))
    } else {
        None
    };

    endpoint.online().await;
    info!(
        id = %endpoint.id(),
        exit = offer_exit,
        proxy = %if run_proxy { args.listen.to_string() } else { "off".into() },
        "teapot is up"
    );

    match proxy_task {
        Some(task) => tokio::select! {
            res = task => res??,
            _ = tokio::signal::ctrl_c() => {}
        },
        None => tokio::signal::ctrl_c().await?,
    }

    if let Some(router) = router {
        router.shutdown().await?;
    }
    endpoint.close().await;
    Ok(())
}

async fn static_exit(key: SecretKey, allow: Allow, allow_lan: bool) -> anyhow::Result<()> {
    let endpoint = teapot_core::bind(key).await?;
    let router = Router::builder(endpoint)
        .accept(
            ALPN_PROXY,
            ExitNode::new(ExitPolicy { allow, allow_lan }, PathBook::default()),
        )
        .spawn();
    router.endpoint().online().await;
    info!(id = %router.endpoint().id(), allow_lan, "exit node ready (static allowlist)");
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}

async fn static_proxy(key: SecretKey, exit: EndpointId, listen: SocketAddr) -> anyhow::Result<()> {
    let endpoint = teapot_core::bind(key).await?;
    info!(id = %endpoint.id(), "client ready");
    let client = Arc::new(ProxyClient::new(
        endpoint.clone(),
        Some(exit),
        PathBook::default(),
    ));
    tokio::select! {
        res = proxy::serve(listen, client) => res?,
        _ = tokio::signal::ctrl_c() => {}
    }
    endpoint.close().await;
    Ok(())
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
    println!("✓ {name} added. Next: `teapot service install` to keep it online.");
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
            (true, false) => "offered, off in dashboard",
            (false, true) => "on in dashboard, not offered by this device right now",
            (false, false) => "off",
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
            println!("\nwaiting for approval; run `teapot login`")
        }
        Err(e) => return Err(e),
    }
    Ok(())
}
