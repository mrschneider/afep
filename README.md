# AFEP — Agentic Federation Protocol

AFEP federates AI agents across administrative domains: typed RPC when
peers can reach each other, a brokered message mesh when they cannot, and a
shared task vocabulary so distinct agent frameworks interoperate.

## The three layers

| Layer | Standard | Where |
|---|---|---|
| Transport | gRPC + Protobuf (HTTP/2) | `proto/afep/v1/transport.proto`, `internal/transport/grpcsvc` |
| Messaging | NATS JetStream | `internal/bus/nats` |
| Semantics | Agent Cards + task lifecycle (A2A-style) | `proto/afep/v1/{agent_card,task}.proto`, `internal/identity`, `internal/lifecycle` |

### Why these pieces

- **gRPC / Protobuf** — rigid schemas: an agent cannot inject unstructured
  fields without failing validation at the serialization boundary.
  Bi-directional streaming carries partial tool output, reasoning chunks
  and execution telemetry over one multiplexed HTTP/2 connection.
- **NATS JetStream** — agents behind NATs or strict firewalls dial out to
  a broker and need no public IP. Durable streams queue task envelopes
  while a site is offline; JetStream KV holds the card directory.
  Superclusters (or leafnodes) merge geographically distributed sites into
  one namespace without custom bridge code.
- **A2A-style semantics** — a machine-readable, self-signed **AgentCard**
  (cryptographic identity, security clearances, capabilities) and a
  **TaskEnvelope** whose lifecycle is PROBE -> BID -> COMMIT -> RUNNING ->
  DONE, with FAILED / ESCALATED for exception escalation across domains.

## Quickstart

```bash
# toolchain: go 1.26+ and the buf codegen plugins
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

make lint test build     # buf lint, go vet, unit tests, binary
docker compose up -d     # local JetStream broker
./bin/afepd              # self card + mesh consumer + gRPC listener
```

Integration test against the live broker:

```bash
docker compose up -d
AFEP_NATS=nats://127.0.0.1:4222 go test ./internal/bus/nats -run Integration -v
```

## Subject scheme

```
afep.<domain>.<agent>.<verb>     verbs: envelopes | bids | telemetry
```

Stream `AFEP_TASKS` captures `afep.>` (file storage, 24h retention, 2min
de-duplication window keyed by task id). Cards live in KV bucket
`afep_cards`. Each site runs one durable consumer filtered to its domain.

## Repo layout

```
proto/afep/v1/                protobuf contracts (buf lint is contract CI)
gen/go/                       generated stubs (committed; CI checks drift)
cmd/afepd/                    reference daemon
internal/identity/            ed25519 card issue/sign/verify
internal/lifecycle/           task state machine
internal/registry/            signature-checked card directory
internal/bus/nats/            JetStream adapter (publish, durable consume, KV)
internal/transport/grpcsvc/   AgentTransportService implementation
docs/ARCHITECTURE.md          technical reference
```

## Status

v0.1.0 — experimental. Go reference implementation; the known gaps are
listed honestly in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
License: MIT.
