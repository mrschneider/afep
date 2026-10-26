# AFEP Architecture

Engineering reference for the v0.1.0 scaffold. Every number here is read
from source: LOC via `grep -c`, tests from `go test`, contracts counted in
`proto/afep/v1/`.

## Layer diagram

```
+---------------------------------------------------------------+
|  Semantic layer (A2A-style)                                    |
|  AgentCard: identity, clearances, capabilities (self-signed)   |
|  TaskEnvelope: PROBE -> BID -> COMMIT -> RUNNING -> DONE       |
|                   FAILED / ESCALATED (cross-domain handoff)    |
+----------------------------+----------------------------------+
                             | protobuf wire format
+----------------------------v----------------------------------+
|  Transport: gRPC over HTTP/2 — service AgentTransportService   |
|  GetCard | Probe (server-stream) | Commit | TaskStream (bi-di) |
+----------------------------+----------------------------------+
                             | envelopes as NATS messages
+----------------------------v----------------------------------+
|  Messaging: NATS JetStream                                     |
|  stream AFEP_TASKS (afep.>) | KV afep_cards | durable consumers |
+---------------------------------------------------------------+
```

## Entrypoint and lifecycle

`cmd/afepd` startup:

1. load-or-create the ed25519 seed (file mode 0600) and issue the
   self-signed AgentCard;
2. `registry.Put(self)` — only signature-verified cards are admitted;
3. optional mesh: Dial JetStream, publish the card to KV, subscribe a
   durable consumer on the site domain. A site without NATS logs
   `mesh disabled` and keeps serving direct gRPC;
4. serve `AgentTransportService`; SIGINT/SIGTERM triggers GracefulStop
   with a 3s hard-stop cap.

A task's life: `Probe` creates a PROBE envelope and streams bids (BID);
`Commit` picks the assignee (COMMIT); RUNNING/DONE/FAILED belong to
executors; ESCALATED re-enters as PROBE in the target domain with
`root_task_id` preserved as the audit chain.

## Packages

Hand-written Go: **1,316 LOC** across 12 files; **20 test functions**;
all packages green under `go test ./...`.

| Package | LOC | Role |
|---|---|---|
| `cmd/afepd` | 138 | flags, key custody, mesh opt-in, shutdown |
| `internal/identity` | 107 | ed25519 keys; card body signed deterministically with the signature field cleared |
| `internal/lifecycle` | 76 | transition table + `Apply` (stamps `updated_at`) |
| `internal/registry` | 66 | signature-checked card map with capability index |
| `internal/bus/nats` | 289 | Dial, stream management, KV cards, dedup publish, durable consume |
| `internal/transport/grpcsvc` | 176 | RPCs over registry + in-memory task table |

Contracts: 3 proto files, 192 LOC, 19 messages/enums/service blocks, 4 RPCs.
Generated stubs in `gen/` are committed; CI regenerates and fails on drift.

## Data model

- **AgentCard** — `id`, raw ed25519 public key, endpoint (gRPC address,
  NATS subject prefix, gateway routes), `capabilities[]` (name/version/
  constraints), `security_clearances[]`, self-signature over the
  deterministic serialization with the signature field cleared.
- **TaskEnvelope** — `task_id` + `root_task_id` audit chain, tenant,
  issuer/assignee, `state`, requested `capability`, typed `payload`
  (`payload_type` media type), `bids[]`, `deadline`, `escalation`,
  issuer signature.
- **Bus** — subjects `afep.<domain>.<agent>.<verb>`; stream `AFEP_TASKS`
  (file storage, 24h max age, 2min de-dup window, `Nats-Msg-Id` =
  task id); KV bucket `afep_cards` with 7d TTL.

## Cross-cutting guarantees

- **Schema boundary** — `buf lint` (STANDARD category) runs in CI on
  every change; protobuf rejects ill-typed fields at the
  serialization boundary, so an agent cannot smuggle unstructured data
  into an envelope.
- **Trust** — every card is signature-verified before admission
  (`registry.Put`); tests re-verify served cards and reject tampered ones.
- **Delivery** — at-least-once with idempotent publish (de-dup key =
  task id), explicit acks, `MaxDeliver=5`, Nak redelivery for handler
  errors, Term for poison messages.
- **Offline tolerance** — the durable consumer buffers while a site is
  disconnected and replays after recovery.
- **Graceful degradation** — the mesh is optional per site; gRPC keeps
  working without a broker.

## Honest notes / gaps (v0.1.0)

- **Bid signing and ranking are not implemented**: `Bid.signature` and
  `score` exist on the wire, but the scaffold returns score 0 in
  registry order and never signs provider bids.
- **Security clearances ride in cards but nothing enforces them** — no
  routing or admission check consults them yet.
- **HTTP/3**: contracts are transport-agnostic, but the default stack is
  HTTP/2; QUIC requires a gRPC setup not wired in this scaffold.
- **Payload schema registry**: `payload_type` is a media-type label;
  actual schema validation against a registry is pending.
- **Task table is in-memory**: a restart loses non-committed tasks; the
  durable path is the JetStream stream, not the gRPC table.
- **No cross-domain router**: ESCALATED re-entry is legal in the state
  machine, but no component performs the inter-domain handoff yet.
- **Single language**: Go only — no Python/TS SDK or interop fixtures yet.
- **Single-node broker**: compose ships one NATS node; supercluster or
  leafnode federation is the intended pattern but not templated here.
- **Not A2A-wire-compatible**: cards and lifecycle are aligned with the
  A2A protocol conceptually; the protobuf contracts are AFEP's own.
