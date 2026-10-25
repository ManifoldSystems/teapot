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

On every device, once:

```bash
teapot login              # opens the approval page; confirm "Add this device?"
teapot service install    # runs `teapot up` in the background, restarts it if it dies
```

`teapot up` keeps one endpoint online and does two jobs:

- **exit node** — serves other devices once you turn **exit** on for this
  device in the dashboard (`--no-exit` to never offer it);
- **local proxy** on `127.0.0.1:1080` — exits through another device: the one
  named with `--exit <name>` while it is online, otherwise any online exit
  node. When the current exit goes offline — or turns out to leave with this
  device's own public IPv4, e.g. a laptop whose hotspot was switched off and
  joined the same Wi-Fi — the proxy moves to another one within seconds and
  comes back to `--exit` when it is useful again (`--no-proxy` to skip the
  proxy).

Options given to `teapot service install` are passed to `up`, e.g.
`teapot service install --exit phone-exit`. `teapot service status` and
`teapot service uninstall` do what they say. macOS uses a launchd agent
(runs while you are logged in); Linux uses a systemd user unit (add
`sudo loginctl enable-linger $USER` to run without a login session).

`teapot exit` and `teapot proxy` are `up --no-proxy` and `up --no-exit`.

**Whole-Mac exit (`--system-proxy`, macOS).** Points the system HTTP/HTTPS
proxy at `127.0.0.1:1080` while an exit node is available, so browsers and
most apps go out through it without per-app settings. With no exit node
online it switches the proxy back off, so the Mac never loses internet
because of teapot. The original settings are saved before anything changes
and restored on shutdown (Ctrl+C or the service stopping), on the next start
after a crash, by `teapot service uninstall`, or by hand with
`teapot system-proxy off`. Needs an admin account (`networksetup`).

**Exit nodes keep working when control is down.** The last allowlist from
control is cached next to the key (`secret.allowlist.json`) and used at
startup until control answers again.

Point apps at `http://127.0.0.1:1080`:

```bash
curl -x http://127.0.0.1:1080 https://ifconfig.me
```

It prints B's public IP. Name a different exit for a single request with the
proxy username, and list the choices from the proxy itself:

```bash
curl -x http://home-exit@127.0.0.1:1080 https://ifconfig.me   # this request via home-exit
curl http://127.0.0.1:1080/_teapot/exits                         # what can be named
```

The Python library [`teapot-py`](https://github.com/ManifoldSystems/teapot-py)
wraps both. A and B on the same network share one IP, so test
across networks (laptop on Wi-Fi, B on a phone hotspot or a VPS).

`teapot status` shows this device's approval, exit state, public IPv4/IPv6
(reported by the agent from its iroh addresses), and its peers.

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
| `--control <url>` / `TEAPOT_CONTROL` | all | Control plane. Default: `https://teapot.example.com` |
| `--name <name>` / `TEAPOT_NAME` | all | Name in the dashboard. Default: hostname |
| `--allow <id>` | exit | Static allowlist, skips control; repeatable |
| `--allow-any` | exit | Any endpoint, skips control. Local experiments only |
| `--allow-lan` | exit | Let clients reach B's loopback and LAN. Off by default |
| `--exit <name>` / `[EXIT]` | up / proxy | Preferred exit node by name, id, or id prefix (≥ 8 chars) |
| `--no-proxy` / `--no-exit` | up | Skip the local proxy / never offer exit |
| `--system-proxy` | up | macOS: route the whole Mac through the proxy while an exit is available |
| `--listen <addr>` | proxy | Local proxy address. Default `127.0.0.1:1080` |
| `--no-control` | proxy | Skip control; `EXIT` must be a full id |
| `--no-browser` | login | Print the approval link instead of opening it |
| `RUST_LOG` | all | e.g. `teapot_core=debug` for per-stream logs |

The proxy logs which path it uses (`path: relay ...` or `path: direct ...`)
whenever it changes. Every device reports its paths and tunnel bytes per peer
(since the agent started) to the dashboard.

## Layout

```
crates/teapot-proto   wire format, ALPN; see docs/protocol.md
crates/teapot-core    exit node, local proxy, control client, exit selection, path book, key, LAN guard
                      tests/tunnel.rs: exit + proxy in one process, no relay or DNS
crates/teapot         CLI, service install (launchd / systemd)
```

## Not yet

- SOCKS5
- `--system-proxy` tried on a real Mac (written and unit-tested; it changes
  system network settings, so run it yourself first)
- systemd unit tried on a Raspberry Pi (runs on Ubuntu 24.04 x86_64)
