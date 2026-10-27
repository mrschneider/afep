# AFEP Protocol Specification (v0.5)

Normative reference for implementing AFEP peers. The protobuf contracts
under `proto/afep/v1/` are the wire source of truth; this document fixes
the *semantics* around them. `[MUST]`/`[MUST NOT]` are binding for
conformance; everything else is explanatory.

## 1. Scope

AFEP is a federation protocol for capability-matched agent tasking:
a domain probes for a capability, agent domains bid, the requester
commits a winner, and unsatisfiable probes escalate peer-to-peer.

Non-goals: payload semantics (opaque to the fabric), transport security
internals (see SECURITY.md), scheduling policy inside a domain.

## 2. Subject namespace

All NATS traffic lives under `afep.<domain>.<agent>.<verb>`:

- Segments `[MUST]` be non-empty and contain none of ` `, `>`, `.`
  (`internal/bus/nats.Subject` enforces this; `/` is allowed inside
  `<agent>`, e.g. `tenant/agent`).
- Verbs: `envelopes` (task lifecycle, JetStream `AFEP_TASKS`),
  `bids` (live core NATS + archive `AFEP_BIDS`), `telemetry`
  (reserved).
- Wildcard: `*` for exactly one segment (`ParseSubject` requires
  exactly four segments; `afep.<domain>.*.<verb>` is the per-domain
  consumer filter).

## 3. Envelope lifecycle

`lifecycle.Apply` enforces this table; same-state refresh is always
legal; `DONE` is terminal.

| From | To |
|---|---|
| UNSPECIFIED | PROBE |
| PROBE | BID, FAILED, ESCALATED |
| BID | COMMIT, PROBE (re-probe), FAILED, ESCALATED |
| COMMIT | RUNNING, FAILED, ESCALATED |
| RUNNING | DONE, FAILED, ESCALATED |
| FAILED | ESCALATED |
| ESCALATED | PROBE (re-entry in target domain) |
| DONE | — (terminal) |

`FAILED -> ESCALATED` exists so a failed attempt inside one domain can
be handed to a peer instead of dying.

## 4. Signatures

All signatures are Ed25519 over the *deterministic* protobuf
serialization (`deterministic=True` / `MarshalOptions{Deterministic:true}`)
of the message with its `signature` field cleared:

- **AgentCard**: whole card minus `signature` (self-signed; verified at
  `registry.Put` admission and again at each envelope verification as
  defense in depth for resolver-sourced cards).
- **Bid**: whole bid minus `signature`; verified against the answering
  card's public key, whose `id` `[MUST]` equal `bid.agent_id`.
- **TaskEnvelope signed core** — the request essence only:

  Covered: `task_id`, `root_task_id`, `tenant`, `issuer_agent`,
  `capability`, `payload_type`, `payload`, `deadline`.

  Excluded (relay-mutable tail): `signature`, `state`,
  `assignee_agent`, `bids`, `updated_at_unix`, `escalation`, `hops`.

  Routers stamp hops/escalations without the issuer key; those fields
  are protected by the dedup key and the audit chain instead. Mutating
  the tail `[MUST NOT]` change one byte of the signed core — locked by
  conformance vectors in both languages.

## 5. Dedup and idempotency

JetStream de-duplicates on `Nats-Msg-Id` **stream-wide** within the
`Duplicates` window (2 min):

- Envelopes: `<task_id>:<state>:<hops>` — each lifecycle hop is its own
  idempotent event (a bare task id would swallow escalations).
- Bids: `<task_id>:<agent_id>`.

At-least-once delivery is assumed everywhere: handlers `[MUST]` be
idempotent per dedup key.

## 6. Ingress rules

Every ingress (gRPC `Probe`/`Commit`/`TaskStream`, mesh consume)
enforces, in order:

1. schema gate — `payload_type` `[MUST]` be registered; payload bytes
   `[MUST]` decode into the registered type with no unknown fields
   (empty payload with a registered type is valid);
2. revocation — if the issuer card id is in the `revoked/<id>` KV set,
   reject (mesh: poison/Term; gRPC: `PermissionDenied`);
3. issuer signature — resolve the issuer card (self or registry/KV) and
   verify the signed core; unknown/unverifiable = reject;
4. clearance — `clearance.CanServe`: grants are `tenant:<pattern>` with
   `*` wildcards; empty tenant passes; Probe admission and Commit winner
   `[MUST]` both check;
5. capability — the serving node `[MUST]` hold the requested capability.

Fail-closed: any lookup error rejects (transient errors requeue, poison
terminates).

## 7. Transports

- **gRPC** (TCP and/or QUIC streams, ALPN `afep-quic/0.1`): RPCs
  `GetCard`, `Probe` (server-streaming bids), `Commit`,
  `TaskStream` (full-duplex envelope/chunk frames).
- **NATS**: `envelopes` on JetStream (24h/1GiB/1M-msg, S2, direct
  reads), `bids` live on core NATS with a best-effort 24h archive;
  cards and revocations in KV `afep_cards` (7d TTL, `WatchAll` mirror).
- **A2A HTTP card**: `/.well-known/agent-card.json` and legacy
  `/.well-known/agent.json` (ETag/304).

## 8. Conformance

`conformance/vectors.json` byte-locks the semantics above (fixed TEST
key): signed bodies, signatures, subject and dedup formulas, and the
tail-exclusion rule. Both `internal/identity/conformance_test.go` and
`python/tests/test_conformance.py` verify them in the normal suites;
`conformance/verify.py` is the standalone gate.

Vectors `[MUST NOT]` change silently: any intentional signed-core or
schema change requires running `conformance/generate.py`, committing the
new vectors in the same release, and bumping the minor version.

## 9. Versioning

- Patch: implementation changes, no wire/semantic delta.
- Minor: additive proto fields, new verbs/sections, or any change that
  forces vector regeneration.
- Breaking removals/renames: `[MUST]` be caught by `buf breaking` in CI
  first and then major-versioned (pre-1.0: minor with explicit notes).