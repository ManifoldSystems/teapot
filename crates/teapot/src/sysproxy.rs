//! Point macOS's system HTTP/HTTPS proxy at the local teapot proxy, and put
//! it back exactly as it was.
//!
//! The original settings are written to disk *before* anything changes, so a
//! crash is undone on the next start, by `teapot service uninstall`, or by
//! `teapot system-proxy off`.

use std::{net::SocketAddr, path::PathBuf, process::Command};

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Setting {
    enabled: bool,
    server: String,
    port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
struct Saved {
    service: String,
    web: Setting,
    secure: Setting,
}

fn state_path() -> anyhow::Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("no config directory")?
        .join("teapot")
        .join("system-proxy.json"))
}

fn networksetup(args: &[&str]) -> anyhow::Result<String> {
    if !cfg!(target_os = "macos") {
        bail!("--system-proxy is only supported on macOS");
    }
    let out = Command::new("networksetup")
        .args(args)
        .output()
        .context("running networksetup")?;
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    // networksetup reports some failures on stdout with status 0.
    if !out.status.success() || stdout.contains("Error") {
        bail!(
            "networksetup {}: {}{}",
            args.first().unwrap_or(&""),
            stdout.trim(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(stdout)
}

/// Enabled network services (`networksetup -listallnetworkservices`).
fn parse_services(out: &str) -> Vec<String> {
    out.lines()
        .skip(1) // "An asterisk (*) denotes that a network service is disabled."
        .filter(|l| !l.is_empty() && !l.starts_with('*'))
        .map(str::to_string)
        .collect()
}

/// Output of `-getwebproxy` / `-getsecurewebproxy`.
fn parse_setting(out: &str) -> Setting {
    let field = |name: &str| {
        out.lines()
            .find_map(|l| l.strip_prefix(name))
            .map(|v| v.trim().to_string())
            .unwrap_or_default()
    };
    Setting {
        enabled: field("Enabled:") == "Yes",
        server: field("Server:"),
        port: field("Port:").parse().unwrap_or(0),
    }
}

fn apply(service: &str, kind: &str, setting: &Setting) -> anyhow::Result<()> {
    // -setwebproxy also switches it on; -set…state off only switches it off.
    if setting.enabled {
        networksetup(&[
            &format!("-set{kind}"),
            service,
            &setting.server,
            &setting.port.to_string(),
        ])?;
    } else {
        networksetup(&[&format!("-set{kind}state"), service, "off"])?;
    }
    Ok(())
}

/// Routes system HTTP and HTTPS through `listen`. No-op if already on.
pub fn enable(listen: SocketAddr) -> anyhow::Result<()> {
    let path = state_path()?;
    if path.exists() {
        return Ok(());
    }
    let services = parse_services(&networksetup(&["-listallnetworkservices"])?);
    let saved: Vec<Saved> = services
        .iter()
        .map(|s| {
            Ok(Saved {
                service: s.clone(),
                web: parse_setting(&networksetup(&["-getwebproxy", s])?),
                secure: parse_setting(&networksetup(&["-getsecurewebproxy", s])?),
            })
        })
        .collect::<anyhow::Result<_>>()?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&saved)?)?;

    let ours = Setting {
        enabled: true,
        server: listen.ip().to_string(),
        port: listen.port(),
    };
    for s in &services {
        if let Err(e) = apply(s, "webproxy", &ours).and_then(|()| apply(s, "securewebproxy", &ours))
        {
            // Leave nothing half-done.
            let _ = disable();
            return Err(e);
        }
    }
    Ok(())
}

/// Restores what [`enable`] saved. No-op if nothing is saved.
pub fn disable() -> anyhow::Result<()> {
    let path = state_path()?;
    let Ok(bytes) = std::fs::read(&path) else {
        return Ok(());
    };
    let saved: Vec<Saved> =
        serde_json::from_slice(&bytes).context("reading saved proxy settings")?;
    let mut first_err = None;
    for s in &saved {
        for (kind, setting) in [("webproxy", &s.web), ("securewebproxy", &s.secure)] {
            if let Err(e) = apply(&s.service, kind, setting) {
                first_err.get_or_insert(e);
            }
        }
    }
    match first_err {
        // Keep the file so a later attempt can still restore.
        Some(e) => Err(e),
        None => {
            std::fs::remove_file(&path)?;
            Ok(())
        }
    }
}

/// Whether teapot currently owns the system proxy.
pub fn is_enabled() -> bool {
    state_path().is_ok_and(|p| p.exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_services() {
        let out = "An asterisk (*) denotes that a network service is disabled.\nEthernet\n*Thunderbolt Bridge\nWi-Fi\n";
        assert_eq!(parse_services(out), ["Ethernet", "Wi-Fi"]);
    }

    #[test]
    fn parses_settings() {
        let off = "Enabled: No\nServer: \nPort: 0\nAuthenticated Proxy Enabled: 0\n";
        assert_eq!(
            parse_setting(off),
            Setting {
                enabled: false,
                server: String::new(),
                port: 0
            }
        );
        let on = "Enabled: Yes\nServer: 127.0.0.1\nPort: 1080\nAuthenticated Proxy Enabled: 0\n";
        assert_eq!(
            parse_setting(on),
            Setting {
                enabled: true,
                server: "127.0.0.1".into(),
                port: 1080
            }
        );
    }
}
