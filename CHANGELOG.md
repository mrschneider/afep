# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0] - 2026-10-26

### Changed

- Taskstore flush cost: protojson runs once per `Put` (per-task `raws`
  cache, bytes preserved from disk at boot) instead of re-encoding every
  stored envelope on each flush — identical full-snapshot tmp+rename
  durability, directory fsync added after the rename.
- Envelope ownership: `taskstore.Put`/`Get` and the gRPC save/load paths
  clone at every boundary, so the documented "callers own the value"
  contract holds and RPC request objects never alias the store; removes
  the Probe/Commit data race caught by `go test -race` (3-4 warnings per
  10 runs before, 0 after).
- NATS connection: explicit reconnect policy (unlimited, 2s wait) with
  logged disconnect/reconnect/close events; `Close()` now drains
  in-flight traffic with a 2s bound instead of dropping it.
- Redelivery: `NakWithDelay` backoff 1s..30s by delivery count instead
  of an immediate NAK loop.
- QUIC certificate persists next to the identity seed
  (`LoadOrCreateCert`, key 0600): stable federation identity across
  restarts instead of a fresh fingerprint per boot.

### Added

- Panic containment: gRPC unary/stream recovery interceptors
  (`codes.Internal` + stack) and `recover()` in the NATS consume
  callback (log + Term) — handler panics no longer kill the daemon.
- gRPC health service on both servers (TCP + QUIC), flipped to
  NOT_SERVING at shutdown (`/healthz` already existed on HTTP).
- `go test -race` gate in CI and in `make test`.
- Taskstore bounds: `-max-tasks` cap (default 100k, 0 = unlimited) so
  `-prune-age=0` is no longer an unbounded store.
- JetStream stream `MaxBytes` (1 GiB) + `DiscardOld`: an inundation
  cannot fill the disk.
- Per-message 10s deadline inside the consume handler (the subscribe
  context had none).

## [0.3.0] - 2026-10-26

### Added

- Issuer-signature enforcement at both envelope ingress paths: the mesh
  handler resolves issuers through a required `CardResolver` (KV-backed in
  afepd) and Terminates unknown/unverifiable envelopes; gRPC `TaskStream`
  rejects them with InvalidArgument / NotFound / PermissionDenied.
- Signed-core envelope semantics (Go + Python, byte-parity): the
  signature covers the request essence (ids, tenant, issuer, capability,
  payload, deadline); the relay-mutable lifecycle tail (state, assignee,
  bids, updated_at, escalation, hops) is excluded so routers can stamp
  hops and escalations without the issuer key.
- Daemon task-store pruning: `-prune-age` (default 24h, 0 disables) with
  a boot prune and an hourly ticker.
- `staticcheck` gate in the go CI job (zero findings at introduction).
- Negative-trust test coverage in both languages: unsigned, unknown
  issuer, and foreign-signature rejection at mesh and transport ingress.

### Changed

- Interop fixtures re-signed under signed-core semantics (both sides
  regenerated in the same change).

## [0.2.0] - 2026-10-26

### Added

- Signed, ranked bids: `SignBid`/`VerifyBid`, deterministic ranking
  (`rank`), multi-node `pkg/client.ProbeAll` with signature verification
  against each serving card.
- Tenant clearance enforcement (`clearance`) at Probe admission and
  Commit winner selection.
- gRPC over QUIC transport (`internal/quictransport`, `-listen-quic`,
  ALPN `afep-quic/0.1`, self-signed TLS 1.3).
- Payload schema registry (`schemareg`) enforced at gRPC ingress and on
  the bus (poison envelopes are Terminated, not redelivered).
- Durable task store (`taskstore`, atomic writes, restart survival,
  `PruneTerminal`, `-state` flag).
- Cross-domain escalation: `router` (hop budget, peer ordering, relay
  subject) and `mesh` handler wired into afepd (`-peers`).
- A2A v1.0 interop: well-known Agent Card with ETag/304 caching at
  `/.well-known/agent-card.json` (+ v0.3 alias) and the 8-state task
  mapping (`internal/a2a`).
- Python reference SDK (`python/afep`) with ed25519 card/bid/envelope
  signing, lifecycle and subject mirrors; cross-language interop
  fixtures verified in both directions (`internal/interop`).
- Two-site federation template (`deploy/federation`, leafnode,
  outbound-only edge) — booted and integration-tested across the link.
- Task envelope issuer signatures (signed on emission) with verifiers in
  both languages; `Bid.task_id` for mesh correlation.
- CI: python job (unit + interop), QUIC/router/mesh/a2a coverage in the
  go job.

### Fixed

- JetStream de-duplicates `Nats-Msg-Id` stream-wide: escalations shared
  the probe's task id and were silently swallowed while `PublishMsg`
  acked. Dedup key is now `task_id:state:hops`.
- Wire-mismatched payload bodies were accepted silently (protobuf parks
  them as unknown fields); `schemareg` now rejects any bytes outside the
  registered schema.

### Changed

- `grpcsvc.New` requires the node's key pair (a node only bids for
  identities whose key it holds); `Probe` returns a signed self-bid
  instead of unsigned registry referrals.
- The stream captures envelope subjects only (`afep.*.*.envelopes`); bids
  travel as ephemeral core-NATS messages.
- Python package requires protobuf runtime >= the committed gencode
  (`protobuf>=7.36.2`), resolved through the repo venv.

## [0.1.0] - 2026-10-25

### Added

- Protobuf contracts (`afep.v1`): AgentCard, TaskEnvelope with the
  PROBE/BID/COMMIT/RUNNING/DONE/FAILED/ESCALATED lifecycle, AgentTransport
  service with bi-directional streaming.
- Go gRPC service scaffold (`internal/transport/grpcsvc`).
- NATS JetStream bus adapter with subject scheme, at-least-once publish
  and KV-backed card bucket (`internal/bus/nats`).
- ed25519 identity: card issue/sign/verify (`internal/identity`).
- Task lifecycle state machine with cross-domain escalation rules
  (`internal/lifecycle`).
- buf lint/codegen, CI workflow, dev compose for JetStream.
