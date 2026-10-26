# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 0.2.x (main) | ✅ current |
| 0.1.x | ❌ superseded |

Only the latest release receives fixes. Pin the git SHA you deploy.

## Reporting a vulnerability

Report privately through **GitHub Security Advisories** on this
repository (Security tab → Report a vulnerability). Do not open a public
issue for exploitable defects. Expect an acknowledgement within a few
days; coordinated disclosure by mutual agreement.

## Security model notes

- **Trust chain**: AgentCards are self-signed (ed25519); bids verify
  against the answering card; emitted envelopes carry the issuer
  signature. Verification helpers live in `internal/identity` (Go) and
  `python/afep/identity.py`.
- **Key custody**: afepd stores a raw ed25519 seed at `-key` with mode
  0600 in a 0700 directory, created by the binary — never commit seeds.
- **Dev-grade defaults to review before production**: the QUIC client
  dials with `InsecureSkipVerify` (no CA yet); `deploy/federation/`
  configs ship without TLS or leafnode authentication and are meant for
  in-network use only; clearance grants default to `tenant:*` via the
  `-clearances` flag — set them explicitly per deployment.
- **Enforcement boundary**: payload schemas are enforced at every
  ingress; envelope issuer signatures are verified by helpers but not yet
  required at ingress (see docs/ARCHITECTURE.md residuals).
