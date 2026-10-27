# AFEP federation — two-site template

A core site (hub) and an edge site that only dials **out** — the shape the
mesh layer was designed for: the edge needs no inbound ports, no public IP
and no reverse proxy.

```
afep-edge ──leafnode (outbound)──▶ afep-core
   │                                 │
   └──────── shared namespace: afep.<domain>.<agent>.<verb>
```

## Boot

```bash
docker compose -f deploy/federation/compose.yml up -d
docker compose -f deploy/federation/compose.yml logs | grep -i leaf
# expect: "leaf node connected" on both sides
```

Host ports: core `4223` (monitor `8223`), edge `4224` (monitor `8224`).
The leaf port (7422) stays inside the compose network — only outbound.

## Verify AFEP traffic across the leaf

```bash
AFEP_NATS=nats://127.0.0.1:4224 \
  go test ./internal/bus/nats -run Integration -v
```

The whole integration suite runs against the **edge** URL: envelopes,
durable consumers and KV cards cross the leaf link to the hub.

## Teardown

```bash
docker compose -f deploy/federation/compose.yml down       # keep volumes
docker compose -f deploy/federation/compose.yml down -v    # wipe state
```

## Scope

Leafnode federation is the templated pattern here. NATS **superclusters**
(gateway blocks between clusters) are the production-scale sibling — same
namespace model, more moving parts; not templated yet (see
ARCHITECTURE.md residuals). Configs are dev-grade: add TLS and
leafnode authentication before anything crosses a real network.


## Backups

- **Task store** (afepd): copy the `-state` file (default
  `<keydir>/tasks.json`) while the daemon is stopped, or rsync it
  periodically — it is an atomic snapshot, any copy is consistent.
- **JetStream**: `docker exec afep-core nats stream backup AFEP_TASKS
  ./backup` and the same for `AFEP_BIDS`; cards and revocations:
  `nats kv backup afep_cards ./backup`. Restore with `nats stream
  restore` / `nats kv restore` against a fresh server.
- **Identity**: the ed25519 seed at `-key` (0600) IS the domain
  identity — back it up offline, never commit it.

## Authentication

Both configs enable user/password auth with CHANGE_ME_* placeholders
(validated with `nats-server -t`). Replace them before deploying and
give afepd the client credentials through the environment:
`AFEP_NATS_USER` / `AFEP_NATS_PASS` (or `AFEP_NATS_TOKEN`,
`AFEP_NATS_CREDS`, `AFEP_NATS_TLSCA`). TLS is still your job.
