# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 0.5.x (main) | ✅ current |
| < 0.5.x | ❌ superseded |

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
  dials with `InsecureSkipVerify` by default — pass `-quic-ca` (pin the
  peer's persisted certificate from `LoadOrCreateCert`) or use
  `quictransport.ClientTLSFromCA` to get real verification;
  `deploy/federation/` configs now ship with user/password auth on the
  client and leafnode ports but still **without TLS** (replace the
  CHANGE_ME_* passwords; hand credentials to afepd via
  `AFEP_NATS_USER`/`AFEP_NATS_PASS`/`AFEP_NATS_CREDS`, never argv);
  clearance grants default to `tenant:*` via the `-clearances` flag —
  set them explicitly per deployment.
- **Enforcement boundary**: payload schemas are enforced at every
  ingress. Issuer signatures are REQUIRED at the mesh consume path
  (`verifyIssuer`: unknown/unverifiable = poison) and at TaskStream
  ingress (`verifyIngressEnvelope`); card revocation is a mirrored KV
  set (`revoked/<id>`, `-revoke-card` to add) checked before
  verification, and the gRPC ingress carries a token-bucket rate limit
  (`-grpc-rate`, default 200 req/s, 0 disables).
