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
