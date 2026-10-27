# AFEP Architecture

Engineering reference for v0.6.0. Every number here is read from source
(LOC via `grep -c`, test counts from `grep "func Test"` / `def test_`,
contracts counted in `proto/afep/v1/`).

## Layer diagram

```
+------------------------------------------------------------------+
|  Semantic layer (A2A-aligned)                                     |
|  AgentCard (ed25519 self-signed) + tenant clearances              |
|  TaskEnvelope: PROBE -> BID -> COMMIT -> RUNNING -> DONE          |
|                  FAILED / ESCALATED (cross-domain, hops<=5)       |
|  A2A v1.0 well-known card + state mapping (internal/a2a)          |
+----------------------------+-------------------------------------+
                             | protobuf wire format
+----------------------------v-------------------------------------+
|  Transport: gRPC over HTTP/2 — service AgentTransportService      |
|  GetCard | Probe (server-stream, signed bids) | Commit | bi-di    |
|  optional: gRPC over QUIC streams (-listen-quic, custom ALPN)     |
+----------------------------+-------------------------------------+
                             | envelopes as NATS messages
+----------------------------v-------------------------------------+
|  Messaging: NATS JetStream                                        |
|  stream AFEP_TASKS (afep.*.*.envelopes) | KV afep_cards           |
|  durable per-domain consumers | router relay afep.<d>.relay.*     |
|  bids: ephemeral core NATS afep.<d>.*.bids (signed)               |
+------------------------------------------------------------------+
```

## Entrypoint and lifecycle

`cmd/afepd` startup: load-or-create the ed25519 seed (0600) -> issue the
self-signed card (clearances/capabilities from flags) -> open the durable
task store and prune stale terminal tasks (`-prune-age`, hourly ticker) -> build the gRPC service (score + store wired) -> optional
mesh (Dial, KV card publish, router peers, durable consumer bound to the
mesh handler) -> serve gRPC on TCP and optionally QUIC -> serve the A2A
well-known card over HTTP -> SIGINT/SIGTERM shuts everything down with a
3s hard-stop cap. Mesh absence logs `mesh disabled` and gRPC keeps serving.

A task's life: `Probe` creates a PROBE envelope, streams at most one
**ed25519-signed self-bid** (capability + clearance gated; zero bids still
carry the task id so the caller can escalate); `Commit` re-checks the
winner's clearance and emits an **issuer-signed** envelope; RUNNING/DONE/
FAILED belong to executors; ESCALATED re-enters as PROBE in the target
domain with `root_task_id` preserved.

## Gap closures (0.1.0 -> 0.2.0)

| v0.1.0 gap | Resolution | Evidence |
|---|---|---|
| Unsigned/unranked bids | `identity.SignBid/VerifyBid`, `rank` (score desc, id asc), `pkg/client.ProbeAll` verifies every bid against its card | `pkg/client` tests: aggregation order, unreachable, **stale-card signature rejected** |
| Clearances not enforced | `clearance.CanServe` gates Probe admission and Commit winner | `TestProbeClearanceDeniedYieldsNoBids` + mesh serve gate |
| HTTP/3 not wired | `internal/quictransport`: gRPC over QUIC streams, self-signed TLS 1.3, ALPN `afep-quic/0.1`, `-listen-quic` | `TestGRPCOverQUIC` (two RPCs, multiplexed), live daemon smoke |
| Payload type was a label | `schemareg`: registered media types, proto/JSON decode, **unknown-field rejection**; enforced at Probe/TaskStream, bus Terms poison | `schemareg` 8 tests + `TestTaskStreamRejects*` |
| In-memory task table | `taskstore`: atomic tmp+rename JSON, restart roundtrip, `PruneTerminal` API, `-state` flag | `TestTaskStoreSurvivesRestart`, taskstore 4 tests |
| No cross-domain router | `router` (hop guards `ErrHopLimit`/`ErrNoPeer`, relay subject) + `mesh` handler (serve / escalate / drop) | unit 8+8 tests; **live E2E**: beta provider escalates to alpha inbox (hops=1) — also verified across the federation leaf |
| Go-only | `python/afep` (identity/lifecycle/subjects) + cross-language fixtures both directions | python 24 tests; `TestPythonFixturesVerify` (Go checks Python sigs); python checks Go sigs |
| No federation template | `deploy/federation`: core hub + outbound-only edge leafnode, host ports 4223/4224 | containers Up, `Leafnode connection created` both sides, full integration suite **PASS via edge URL** |
| Not A2A-wire-aware | `internal/a2a`: v1.0 Agent Card (required fields, supportedInterfaces, skills, ETag/304 caching) at both well-known URIs + 8-state mapping (spec-grounded) | a2a 8 tests incl. conditional GET |

Wire-level bug found and fixed by the E2E: JetStream de-duplicates
`Nats-Msg-Id` **stream-wide**, so an escalation sharing its probe's task id
was silently swallowed (`PublishMsg` still acks duplicates). The dedup key
is now `task_id:state:hops` — each lifecycle hop is its own idempotent
event.

## Packages

Hand-written Go: **6,405 LOC** across 17 packages, **95 test functions**;
Python reference: **814 LOC**, **30 tests**; contracts: 4 proto files,
209 LOC, 20 messages/enums/service blocks, 4 RPCs.

| Package | LOC | Tests | Role |
|---|---|---|---|
| `cmd/afepd` | 396 | - | flags, key custody, mesh/QUIC/HTTP wiring, prune loop |
| `internal/identity` | 729 | 17 | card/bid/envelope signing (deterministic body) |
| `internal/transport/grpcsvc` | 831 | 11 | RPCs: signed bids, clearances, schema gate, store |
| `internal/bus/nats` | 1106 | 7 | stream/KV/bids, durable consume, Term-on-poison |
| `internal/a2a` | 362 | 8 | A2A card derivation, state map, well-known handler |
| `internal/mesh` | 628 | 11 | inbound policy: verify issuer, serve / escalate / drop |
| `internal/quictransport` | 536 | 5 | QUIC listener/dialer, stream->net.Conn, TLS bootstrap |
| `internal/router` | 273 | 8 | escalation with hop budget and peer ordering |
| `internal/interop` | 292 | 3 | fixture generation + cross-language verification |
| `internal/taskstore` | 353 | 4 | atomic persistence + terminal pruning |
| `pkg/client` | 242 | 3 | ProbeAll: fan-out, verify, rank |
| `internal/metrics` | 55 | - | Prometheus text counters, no client dep |
| `internal/schemareg` | 189 | 8 | media-type registry and payload validation |
| `internal/lifecycle` | 149 | 3 | transition table + Apply |
| `internal/registry` | 130 | 3 | verified card directory + capability index |
| `internal/clearance` | 88 | 2 | grant/wildcard matching, tenant admission |
| `internal/rank` | 46 | 2 | deterministic bid ordering |

Generated stubs in `gen/` are committed; CI regenerates and fails on
drift. Python stubs (`python/afep/v1/`) are regenerated with
`buf generate --template buf.gen.python.yaml`; wire-shape drift is caught
by the interop fixtures (Go verification fails when the schema moves).

## Data model

- **AgentCard** — id, raw ed25519 key, endpoint (gRPC addr, NATS prefix,
  gateways), capabilities, `security_clearances`, self-signature over the
  deterministic serialization with the signature cleared.
- **TaskEnvelope** — `task_id`/`root_task_id` audit chain, tenant,
  issuer/assignee, state, capability + `payload_type`/`payload` validated
  at every ingress, bids, deadline, escalation, `hops`.
  **Signature semantics (signed core)**: the issuer signs the request
  essence (ids, tenant, issuer, capability, payload, deadline); the
  relay-mutable tail (state, assignee, bids, updated_at, escalation,
  hops) is excluded so routers stamp hops/escalations without the issuer
  key. Both ingresses enforce it: the mesh resolves the issuer from the
  KV directory (unknown/unverifiable -> Term), gRPC `TaskStream` from
  self/registry (InvalidArgument / NotFound / PermissionDenied).
- **Bus** — stream `AFEP_TASKS` (subjects `afep.*.*.envelopes`, file,
  24h, 1 GiB/1M-msg caps, S2 compression, direct reads, 2min dedup, key
  `task_id:state:hops`); stream `AFEP_BIDS` (24h best-effort archive,
  256 MiB/500k); KV `afep_cards` (7d TTL) behind a `WatchAll`-fed
  mirror (zero-RTT issuer lookups); bids live on core NATS with key
  `task:agent`; the durable consumer starts at the tail (DeliverNew),
  AckWait 60s, server BackOff 60s..30m, MaxAckPending 100.
- **Task store** — JSON file (protojson), atomic replace, optional
  `PruneTerminal(maxAge)`.

## Cross-cutting guarantees

- **Schema boundary** — buf STANDARD lint in CI; protobuf rejects bad
  fields at (de)serialization; `schemareg` additionally rejects
  unregistered media types and any bytes outside the schema (unknown
  fields); the bus Terms poison envelopes instead of looping redelivery.
- **Trust chain** — cards verify on admission (`registry.Put`), bids
  verify against the answering card, emitted envelopes carry the issuer
  signature, and **both ingress paths enforce it** (mesh via KV resolver,
  transport via self/registry); stale/forged artifacts are rejected in
  tests both languages.
- **Delivery** — at-least-once for envelopes with hop-aware idempotent
  publish; explicit acks, MaxDeliver=5, Nak on handler error; bids are
  at-most-once by design (re-probe instead of replay).
- **Loop safety** — router never re-enters the arrival domain, drops at
  `ErrNoPeer`/`ErrHopLimit` (default 5 hops), audits via
  `root_task_id` + escalation chain.
- **Offline tolerance** — durable consumers buffer per domain while a
  site disconnects and replay on recovery (proven across the leaf link).
- **Graceful degradation** — no broker => gRPC-only; no QUIC flag =>
  TCP-only; HTTP card optional.
- **Interop** — Go and Python produce byte-identical signed artifacts
  (deterministic protobuf), verified in both directions from committed
  fixtures.

## Honest notes / residuals (v0.2.0)

- **HTTP/3**: what ships is gRPC over QUIC streams with custom ALPN
  `afep-quic/0.1` — grpc-go has no production HTTP/3 transport yet, so
  this is not standardized h3. The client dials with
  `InsecureSkipVerify` (dev scaffold): pin a CA or add mTLS before real
  networks.
- **A2A scope**: discovery-level only — Agent Card + task-state
  vocabulary. The A2A task RPC surface (`SendMessage` and friends) and
  RFC 8785 JWS card signatures are not implemented; the AFEP ed25519 card
  remains the trust anchor.
- **Federation template is leafnode pair** (the outbound-only pattern);
  NATS superclusters (gateway blocks) are not templated. Configs are
  dev-grade: no TLS or leaf authentication — keep them in-network.
- **Ranking score is a static flag** (`-bid-score`); no load- or
  latency-aware scoring yet.
- **Foreign direct-gRPC issuers must already be in the local registry**:
  the mesh resolves issuers from the KV directory (complete), while
  `TaskStream` resolves from self/registry only — a KV->registry sync
  for foreign peers dialing gRPC directly is pending.
- **Python stub regeneration is manual** after proto changes; the Go
  stubs are CI-drift-checked, and the interop fixtures fail loudly when
  wire shapes diverge. Pyright reports false positives on generated pb2
  modules (runtime and CI are green).
- **Bids have no replay (live path)**: core-NATS ephemeral delivery
  means a live collector that starts late misses bids — by design,
  re-probe; `AFEP_BIDS` keeps a 24h best-effort archive for rank
  reconstruction and ops.
