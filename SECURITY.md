# Security

## Supported versions

Security fixes target the current `main` branch. There is no separate long-term
support release line. Teapot is experimental software; a secret scan is not a
security review of its networking or authentication design.

## Reporting a vulnerability

Use **Security → Report a vulnerability** on GitHub when private reporting is
available. If that option is unavailable, open an issue titled **Private security
contact requested** without technical details, credentials, or exploit payloads;
a maintainer can arrange a private reporting channel.

Privately include the affected commit/version, reproduction steps, expected and
actual behavior, and impact. Use a test deployment and redact tokens, device
keys, network addresses, and personal data from logs.

## Deployment boundaries

Deploy your own control plane and configure its URL explicitly. Device approval
and exit enablement are separate decisions. Keep `--allow-lan` disabled unless
you intend to expose the exit device's local network to approved peers. Bind the
local proxy to loopback unless you deliberately provide access control around it.

All approved devices can view mesh diagnostic logs. Treat device keys, dashboard
credentials, local state, and diagnostic logs as private deployment data.
