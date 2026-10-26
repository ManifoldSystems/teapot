use anyhow::{Context, bail};
use clap::Args;
use std::{io::Write, path::Path, time::Duration};
use teapot_core::{
    control::ControlClient,
    logs::{LogEntry, StoredLog},
};

#[derive(Args)]
pub struct LogArgs {
    /// Remote name, full node id, id prefix, or "control". Default: local files.
    device: Option<String>,
    /// Watch every device and control.
    #[arg(long, conflicts_with = "device")]
    all: bool,
    #[arg(short, long)]
    follow: bool,
    #[arg(long, default_value = "info", value_parser = ["debug", "info", "warn", "error"])]
    level: String,
    /// Emit JSON lines instead of formatted text.
    #[arg(long)]
    json: bool,
}

fn print(entry: &LogEntry, node: &str, json: bool) {
    let mut stdout = std::io::stdout().lock();
    let result = if json {
        writeln!(
            stdout,
            "{}",
            serde_json::json!({"node_id": node, "id": entry.id, "timestamp": entry.timestamp, "level": entry.level, "source": entry.source, "message": entry.message})
        )
    } else {
        writeln!(
            stdout,
            "{} {:5} [{}] {}: {}",
            entry.timestamp,
            entry.level.to_uppercase(),
            &node[..node.len().min(8)],
            entry.source,
            entry.message
        )
    };
    if let Err(error) = result {
        if error.kind() == std::io::ErrorKind::BrokenPipe {
            // A viewer pipeline such as `logs | head` is finished successfully.
            std::process::exit(0);
        }
        eprintln!("writing logs: {error}");
        std::process::exit(1);
    }
}

pub async fn run(
    base: &str,
    key: iroh::SecretKey,
    key_path: &Path,
    args: &LogArgs,
) -> anyhow::Result<()> {
    if args.device.is_none() && !args.all {
        let file = key_path.with_extension("log.jsonl");
        let levels = ["debug", "info", "warn", "error"];
        let minimum = levels.iter().position(|l| *l == args.level).unwrap_or(1);
        let mut last_id = String::new();
        loop {
            let mut entries = Vec::new();
            for p in [file.with_extension("jsonl.1"), file.clone()] {
                if let Ok(text) = std::fs::read_to_string(p) {
                    entries.extend(
                        text.lines()
                            .filter_map(|s| serde_json::from_str::<LogEntry>(s).ok()),
                    );
                }
            }
            let start = entries
                .iter()
                .position(|e| e.id == last_id)
                .map(|i| i + 1)
                .unwrap_or_else(|| entries.len().saturating_sub(200));
            for e in &entries[start..] {
                if levels.iter().position(|l| *l == e.level).unwrap_or(0) >= minimum {
                    print(e, "local", args.json);
                }
                last_id.clone_from(&e.id);
            }
            if !args.follow {
                return Ok(());
            }
            tokio::select! { _ = tokio::signal::ctrl_c() => return Ok(()), _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
        }
    }
    let client = ControlClient::new(base, key)?;
    let device = if let Some(wanted) = &args.device {
        if wanted == "control" || wanted == &client.node_id() {
            Some(wanted.clone())
        } else {
            let peers = client.peers().await?;
            let matches: Vec<_> = peers.iter().filter(|p| p.matches(wanted)).collect();
            if matches.len() > 1 {
                bail!("device name/id prefix is ambiguous");
            }
            Some(matches.first().context("device not found")?.node_id.clone())
        }
    } else {
        None
    };
    let show = |e: StoredLog| print(&e.entry, &e.node_id, args.json);
    if !args.follow {
        for e in client.logs(device.as_deref(), &args.level).await? {
            show(e);
        }
        return Ok(());
    }
    let mut cursor = 0;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            result = client.follow_logs(device.as_deref(), &args.level, &mut cursor, show) => {
                if let Err(e) = result { eprintln!("{e:#}; retrying in 3s"); }
            }
        }
        tokio::select! { _ = tokio::signal::ctrl_c() => return Ok(()), _ = tokio::time::sleep(Duration::from_secs(3)) => {} }
    }
}
