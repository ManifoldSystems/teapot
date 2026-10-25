//! `teapot up` as a background service: a launchd agent on macOS, a systemd
//! user unit on Linux. Both restart it when it exits.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail};

use crate::{DEFAULT_CONTROL, UpArgs};

const LABEL: &str = "com.manifoldsystems.teapot";
const DEFAULT_LISTEN: &str = "127.0.0.1:1080";

/// Global flags worth baking into the service command line.
pub fn global_args(key: &Option<PathBuf>, control: &str, name: &Option<String>) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(key) = key {
        // The service's working directory is not ours.
        let key = std::path::absolute(key).unwrap_or_else(|_| key.clone());
        out.extend(["--key".into(), key.display().to_string()]);
    }
    if control != DEFAULT_CONTROL {
        out.extend(["--control".into(), control.to_string()]);
    }
    if let Some(name) = name {
        out.extend(["--name".into(), name.clone()]);
    }
    out
}

fn up_args(args: &UpArgs) -> Vec<String> {
    let mut out = vec!["up".to_string()];
    if let Some(exit) = &args.exit {
        out.extend(["--exit".into(), exit.clone()]);
    }
    if args.listen.to_string() != DEFAULT_LISTEN {
        out.extend(["--listen".into(), args.listen.to_string()]);
    }
    if args.no_proxy {
        out.push("--no-proxy".into());
    }
    if args.no_exit {
        out.push("--no-exit".into());
    }
    if args.allow_lan {
        out.push("--allow-lan".into());
    }
    out
}

pub fn install(global: &[String], args: &UpArgs) -> anyhow::Result<()> {
    let exe = std::env::current_exe()
        .and_then(|p| p.canonicalize())
        .context("locating the teapot binary")?;
    let mut argv = vec![exe.display().to_string()];
    argv.extend_from_slice(global);
    argv.extend(up_args(args));

    platform::install(&argv)?;
    println!("Service installed: {}", argv[1..].join(" "));
    println!("Binary: {}", exe.display());
    println!("Replacing that binary later? Run `teapot service install` again to restart it.");
    Ok(())
}

pub fn uninstall() -> anyhow::Result<()> {
    platform::uninstall()
}

pub fn status() -> anyhow::Result<()> {
    platform::status()
}

fn home() -> anyhow::Result<PathBuf> {
    dirs::home_dir().context("no home directory")
}

fn run(cmd: &mut Command) -> anyhow::Result<String> {
    let out = cmd
        .output()
        .with_context(|| format!("running {:?}", cmd.get_program()))?;
    if !out.status.success() {
        bail!(
            "{:?} failed: {}",
            cmd.get_program(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn tail(path: &Path, lines: usize) -> String {
    std::fs::read_to_string(path)
        .map(|s| {
            let all: Vec<&str> = s.lines().collect();
            all[all.len().saturating_sub(lines)..].join("\n")
        })
        .unwrap_or_default()
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

    fn plist_path() -> anyhow::Result<PathBuf> {
        Ok(home()?
            .join("Library/LaunchAgents")
            .join(format!("{LABEL}.plist")))
    }

    fn log_path() -> anyhow::Result<PathBuf> {
        Ok(home()?.join("Library/Logs/teapot.log"))
    }

    fn domain() -> anyhow::Result<String> {
        Ok(format!("gui/{}", run(Command::new("id").arg("-u"))?.trim()))
    }

    fn xml(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }

    pub fn install(argv: &[String]) -> anyhow::Result<()> {
        let plist = plist_path()?;
        let log = log_path()?;
        let args: String = argv
            .iter()
            .map(|a| format!("    <string>{}</string>\n", xml(a)))
            .collect();
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array>
{args}  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>NO_COLOR</key>
    <string>1</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
</dict>
</plist>
"#,
            log = xml(&log.display().to_string()),
        );

        let domain = domain()?;
        unload(&domain);
        if let Some(dir) = plist.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&plist, body).with_context(|| format!("writing {}", plist.display()))?;
        run(Command::new("launchctl")
            .args(["bootstrap", &domain])
            .arg(&plist))?;
        println!("launchd agent {LABEL} loaded; logs: {}", log.display());
        println!("It runs while you are logged in to this Mac.");
        Ok(())
    }

    /// Stops a loaded copy and waits until launchd has let go of it;
    /// bootstrapping too early fails with "Input/output error".
    fn unload(domain: &str) {
        let target = format!("{domain}/{LABEL}");
        if run(Command::new("launchctl").args(["bootout", &target])).is_err() {
            return; // nothing was loaded
        }
        for _ in 0..50 {
            if run(Command::new("launchctl").args(["print", &target])).is_err() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }

    pub fn uninstall() -> anyhow::Result<()> {
        let plist = plist_path()?;
        unload(&domain()?);
        if plist.exists() {
            std::fs::remove_file(&plist)?;
            println!("Removed {}", plist.display());
        } else {
            println!("No service installed.");
        }
        Ok(())
    }

    pub fn status() -> anyhow::Result<()> {
        let plist = plist_path()?;
        if !plist.exists() {
            println!("not installed (run `teapot service install`)");
            return Ok(());
        }
        let domain = domain()?;
        match run(Command::new("launchctl").args(["print", &format!("{domain}/{LABEL}")])) {
            Ok(out) => {
                for line in out.lines().map(str::trim) {
                    if line.starts_with("state =")
                        || line.starts_with("pid =")
                        || line.starts_with("runs =")
                    {
                        println!("{line}");
                    }
                }
            }
            Err(_) => println!("installed but not loaded"),
        }
        let log = log_path()?;
        println!("\n{}:\n{}", log.display(), tail(&log, 5));
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;

    const UNIT: &str = "teapot.service";

    fn unit_path() -> anyhow::Result<PathBuf> {
        Ok(home()?.join(".config/systemd/user").join(UNIT))
    }

    /// systemd splits ExecStart on spaces; quote every argument.
    fn quote(s: &str) -> String {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    }

    pub fn install(argv: &[String]) -> anyhow::Result<()> {
        let unit = unit_path()?;
        let exec: Vec<String> = argv.iter().map(|a| quote(a)).collect();
        let body = format!(
            "[Unit]\nDescription=Teapot mesh agent\nAfter=network-online.target\nWants=network-online.target\n\n\
             [Service]\nExecStart={}\nRestart=always\nRestartSec=5\nEnvironment=NO_COLOR=1\n\n\
             [Install]\nWantedBy=default.target\n",
            exec.join(" ")
        );
        if let Some(dir) = unit.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&unit, body).with_context(|| format!("writing {}", unit.display()))?;
        run(Command::new("systemctl").args(["--user", "daemon-reload"]))?;
        run(Command::new("systemctl").args(["--user", "enable", UNIT]))?;
        run(Command::new("systemctl").args(["--user", "restart", UNIT]))?;
        println!("systemd user unit {UNIT} started; logs: journalctl --user -u teapot -f");

        let user = std::env::var("USER").unwrap_or_default();
        let linger = run(Command::new("loginctl").args(["show-user", &user, "-p", "Linger"]))
            .unwrap_or_default();
        if !linger.contains("Linger=yes") {
            println!("To keep it running without a login session (e.g. a Raspberry Pi):");
            println!("  sudo loginctl enable-linger {user}");
        }
        Ok(())
    }

    pub fn uninstall() -> anyhow::Result<()> {
        let unit = unit_path()?;
        let _ = run(Command::new("systemctl").args(["--user", "disable", "--now", UNIT]));
        if unit.exists() {
            std::fs::remove_file(&unit)?;
            let _ = run(Command::new("systemctl").args(["--user", "daemon-reload"]));
            println!("Removed {}", unit.display());
        } else {
            println!("No service installed.");
        }
        Ok(())
    }

    pub fn status() -> anyhow::Result<()> {
        if !unit_path()?.exists() {
            println!("not installed (run `teapot service install`)");
            return Ok(());
        }
        let out = Command::new("systemctl")
            .args(["--user", "status", "--no-pager", "-n", "5", UNIT])
            .output()?;
        print!("{}", String::from_utf8_lossy(&out.stdout));
        Ok(())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod platform {
    use super::*;

    pub fn install(_: &[String]) -> anyhow::Result<()> {
        bail!("services are supported on macOS and Linux only; run `teapot up` yourself")
    }
    pub fn uninstall() -> anyhow::Result<()> {
        bail!("services are supported on macOS and Linux only")
    }
    pub fn status() -> anyhow::Result<()> {
        bail!("services are supported on macOS and Linux only")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> UpArgs {
        UpArgs {
            exit: None,
            listen: DEFAULT_LISTEN.parse().unwrap(),
            no_proxy: false,
            no_exit: false,
            allow_lan: false,
        }
    }

    #[test]
    fn defaults_stay_off_the_command_line() {
        assert_eq!(up_args(&args()), ["up"]);
        assert!(global_args(&None, DEFAULT_CONTROL, &None).is_empty());
    }

    #[test]
    fn options_are_carried_over() {
        let a = UpArgs {
            exit: Some("phone-exit".into()),
            listen: "127.0.0.1:8080".parse().unwrap(),
            no_exit: true,
            allow_lan: true,
            ..args()
        };
        assert_eq!(
            up_args(&a),
            [
                "up",
                "--exit",
                "phone-exit",
                "--listen",
                "127.0.0.1:8080",
                "--no-exit",
                "--allow-lan"
            ]
        );
        let g = global_args(&None, "https://example.com", &Some("raspi".into()));
        assert_eq!(g, ["--control", "https://example.com", "--name", "raspi"]);
    }
}
