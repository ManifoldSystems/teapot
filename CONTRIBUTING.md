# Contributing

Teapot is a self-hosted, single-tenant mesh. Small, focused pull requests are
welcome. Open an issue first for changes to the architecture or wire protocol.

## Development

Install Rust 1.98.1 or newer, then run:

```sh
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

CI runs the Rust checks on Linux and macOS. Tunnel integration tests use local
endpoints; no production deployment is needed. Use your own control plane for
manual testing and set `TEAPOT_CONTROL` explicitly.

## Pull requests

- Explain the behavior change and how you verified it.
- Update `docs/protocol.md` first for wire changes, and coordinate the matching
  changes in `teapot-control` and `teapot-app`.
- Use example domains, documentation IPs, and fictional device names in fixtures.
  Keep credentials, deployment IDs, and real fleet details out of commits and logs.
- Add focused regression tests for behavior changes. Preserve third-party notices
  and identify the source and license of any reused code.

Contributions are provided under this repository's MIT license.

For vulnerabilities, see [SECURITY.md](SECURITY.md).
