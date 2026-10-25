# Teapot

Personal mesh on [iroh](https://iroh.computer). First feature: **exit node** —
send traffic from device A and have the internet see device B's IP.

```
app on A ──► teapot proxy (localhost:1080) ══ iroh ══► teapot exit on B ──► internet
                                       direct, or n0 relay when hole punching fails

            both log in to teapot-control (Cloudflare Workers):
            approval, exit allowlist, presence, dashboard
```

Devices find each other by endpoint id through iroh's DNS lookup. Proxy traffic
goes device to device; [teapot-control](https://github.com/ManifoldSystems/teapot-control)
only decides who may talk to whom.

## Build

```bash
cargo build --release
```

Binary: `target/release/teapot`.

## Use

Add each device once:

```bash
teapot login
```

It opens the control plane's approval page for this device (sign in with the
admin token if needed), shows the device's name, platform and code, and asks
**Add this device?** Confirm, and the command finishes on its own.
`--no-browser` prints the link instead.

On the exit node (B), then turn **exit** on for it in the dashboard:

```bash
teapot exit
```

On the client (A):

```bash
teapot proxy
```

B picks up the allowlist immediately; A picks the first online exit node (or
name one: `teapot proxy raspi`). Skipping `teapot login` works too: `exit` and
`proxy` register the device as pending and print the approval link.

Point apps at `http://127.0.0.1:1080`:

```bash
curl -x http://127.0.0.1:1080 https://ifconfig.me
```

It prints B's public IP. A and B on the same network share one IP, so test
across networks (laptop on Wi-Fi, B on a phone hotspot or a VPS).

`teapot status` shows this device's approval, exit state, last public IP, and
its peers.

Revoking or removing a device in the dashboard disconnects it from every exit
node within a second or two.

### Without control

```bash
teapot id                                  # on A
teapot exit --allow <A_ID>                 # on B, prints B's id
teapot proxy --no-control <B_ID>           # on A
```

## Options

| Flag | Where | Meaning |
|---|---|---|
| `--key <path>` / `TEAPOT_KEY` | all | Secret key file. Default: `<config dir>/teapot/secret.key` |
| `--control <url>` / `TEAPOT_CONTROL` | all | Control plane. Default: the deployed `teapot-control` |
| `--name <name>` / `TEAPOT_NAME` | all | Name in the dashboard. Default: hostname |
| `--allow <id>` | exit | Static allowlist, skips control; repeatable |
| `--allow-any` | exit | Any endpoint, skips control. Local experiments only |
| `--allow-lan` | exit | Let clients reach B's loopback and LAN. Off by default |
| `[EXIT]` | proxy | Exit node by name, id, or id prefix (≥ 8 chars) |
| `--listen <addr>` | proxy | Local proxy address. Default `127.0.0.1:1080` |
| `--no-control` | proxy | Skip control; `EXIT` must be a full id |
| `--no-browser` | login | Print the approval link instead of opening it |
| `RUST_LOG` | all | e.g. `teapot_core=debug` for per-stream logs |

The proxy logs which path it uses (`path: relay ...` or `path: direct ...`)
whenever it changes, and every device reports its paths to the dashboard.

## Layout

```
crates/teapot-proto   wire format, ALPN; see docs/protocol.md
crates/teapot-core    exit node, local proxy, control client, path book, key storage, LAN guard
crates/teapot         CLI
```

## Run as a service (macOS)

`packaging/macos/com.manifoldsystems.teapot.plist` runs `teapot exit` with
`RunAtLoad` and `KeepAlive`, so a Mac stays online whenever its user is logged
in and launchd restarts it if it dies. Replace `__HOME__`, copy it to
`~/Library/LaunchAgents/`, then load it. Logs go to `~/Library/Logs/teapot.log`.

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.manifoldsystems.teapot.plist
launchctl kickstart -k gui/$(id -u)/com.manifoldsystems.teapot   # restart after upgrading
```

## Not yet

- SOCKS5
- systemd unit for Linux / Raspberry Pi
- Switching to another exit node when the chosen one goes offline
