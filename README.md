# AFEP — Agentic Federation Protocol

[![ci](https://github.com/mrschneider/afep/actions/workflows/ci.yml/badge.svg)](https://github.com/mrschneider/afep/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Python](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white)](python/)

AFEP federates AI agents across administrative domains: typed RPC when
peers can reach each other, a brokered message mesh when they cannot, and a
shared task vocabulary so distinct agent frameworks interoperate.

## The three layers

| Layer | Standard | Where |
|---|---|---|
| Transport | gRPC + Protobuf (HTTP/2, optional QUIC) | `proto/afep/v1/transport.proto`, `internal/transport/grpcsvc`, `internal/quictransport` |
| Messaging | NATS JetStream + cross-domain router | `internal/bus/nats`, `internal/router`, `internal/mesh` |
| Semantics | Agent Cards + task lifecycle (A2A-aligned) | `internal/identity`, `internal/lifecycle`, `internal/clearance`, `internal/a2a` |

### Why these pieces

- **gRPC / Protobuf** — rigid schemas: an agent cannot inject unstructured
  fields without failing validation at the serialization boundary.
  Bi-directional streaming carries partial tool output and telemetry over
  one multiplexed connection; gRPC can additionally ride **QUIC streams**
  (`-listen-quic`) for NAT-friendly, migration-capable links.
- **NATS JetStream** — agents behind NATs dial out to a broker and need no
  public IP. Durable streams queue task envelopes while a site is offline;
  KV holds the card directory; the **router** relays escalations between
  domains with hop guards (`deploy/federation/` ships a two-site leafnode
  template that boots and passes the full integration suite).
- **A2A-style semantics** — a self-signed **AgentCard** (ed25519 identity,
  security clearances, capabilities), a **TaskEnvelope** lifecycle
  PROBE → BID → COMMIT → RUNNING → DONE with FAILED / ESCALATED handoff,
  signed bids ranked deterministically, and an **A2A v1.0 well-known card**
  at `/.well-known/agent-card.json` for ecosystem discovery.

## Quickstart

```bash
# toolchain: go 1.26+ and the buf codegen plugins
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

make lint test build     # buf lint, go vet, unit tests, binary
docker compose up -d     # local JetStream broker
./bin/afepd              # self card + mesh consumer + gRPC + A2A card
```

Python reference (interop fixtures, no broker needed):

```bash
python3 -m venv .venv && .venv/bin/pip install -e ./python
cd python && ../.venv/bin/python -m unittest discover -s tests -v
```

Two-site federation (leafnode, outbound-only edge):

```bash
docker compose -f deploy/federation/compose.yml up -d
AFEP_NATS=nats://127.0.0.1:4224 go test ./internal/bus/nats -run Integration -v
```

## Daemon flags

```
-listen        gRPC TCP address          (default 127.0.0.1:7700)
-listen-quic   gRPC over QUIC address    (empty = disabled)
-http-listen   A2A well-known HTTP       (default 127.0.0.1:7701, empty = disabled)
-nats          NATS URL                  (empty = mesh disabled, gRPC still serves)
-id/-domain    self card identity / subject domain
-key/-state    ed25519 seed (0600) / durable task store file
-clearances    comma list, e.g. "tenant:*,domain:local"
-capability    comma list of advertised capabilities
-bid-score     score stamped on this node's bids
-peers         comma list of peer domains for cross-domain escalation
-prune-age     prune terminal tasks older than this (default 24h, 0 = off)
```

## Subject scheme

```
afep.<domain>.<agent>.<verb>     verbs: envelopes | bids | telemetry
```

Stream `AFEP_TASKS` captures envelopes only (`afep.*.*.envelopes`, file
storage, 24h, de-dup window 2min keyed by `task_id:state:hops` — hop-aware
so escalations never collide with their probe). Bids travel as ephemeral
core-NATS messages (`afep.<domain>.*.bids`, de-dup key `task:agent`);
cards live in KV bucket `afep_cards`. Each site runs one durable consumer
filtered to its domain.

## Repo layout

```
proto/afep/v1/                protobuf contracts (buf lint is contract CI)
gen/go/                       generated Go stubs (committed; CI checks drift)
cmd/afepd/                    reference daemon
internal/identity/            ed25519 card/bid/envelope signing
internal/lifecycle/           task state machine
internal/clearance/           tenant admission policy
internal/rank/                deterministic bid ranking
internal/schemareg/           payload schema boundary
internal/taskstore/           durable task state (atomic file)
internal/registry/            signature-checked card directory
internal/bus/nats/            JetStream adapter (envelopes, KV, bids)
internal/router/              cross-domain escalation with hop guards
internal/mesh/                envelope handler: serve signed bids / escalate
internal/transport/grpcsvc/   AgentTransportService implementation
internal/quictransport/       gRPC over QUIC (custom ALPN)
internal/a2a/                 A2A v1.0 card + state mapping (well-known)
pkg/client/                   multi-node probe: verify + rank
internal/interop/             cross-language fixture verification
python/                       Python reference SDK + interop fixtures
deploy/federation/            two-site leafnode template
docs/ARCHITECTURE.md          technical reference (incl. honest residuals)
```

## Status

v0.4.0 — experimental. All v0.1.0 gaps are closed; the current honest
residuals live in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
License: MIT.
