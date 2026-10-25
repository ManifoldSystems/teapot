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

On the exit node (B):

```bash
teapot exit
```

On the client (A):

```bash
teapot proxy
```

Both log in to control with their key and show up in the dashboard as
**pending**. Approve both there and turn **exit** on for B. B picks up the
allowlist immediately; A waits for approval, then picks the first online exit
node (or name one: `teapot proxy raspi`).

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
| `RUST_LOG` | all | e.g. `teapot_core=debug` for per-stream logs |

The proxy logs which path it uses (`path: relay ...` or `path: direct ...`)
whenever it changes, and every device reports its paths to the dashboard.

## Layout

```
crates/teapot-proto   wire format, ALPN; see docs/protocol.md
crates/teapot-core    exit node, local proxy, control client, path book, key storage, LAN guard
crates/teapot         CLI
```

## Not yet

- SOCKS5
- Running as a service (systemd / launchd)
- Switching to another exit node when the chosen one goes offline
