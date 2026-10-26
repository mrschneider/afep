# AFEP Python reference

Python mirror of the AFEP trust core: ed25519 card/bid/envelope signing,
the task lifecycle state machine and the NATS subject scheme — byte-level
compatible with the Go implementation (interop fixtures in `testdata/`).

```bash
pip install -e .            # deps: protobuf, cryptography
python -m unittest discover -s tests -v
```

Interop regeneration (writes `testdata/`):

```bash
AFEP_FIXTURES=1 python -m unittest tests.test_interop -v
```

Scope: wire/identity parity only — no gRPC or JetStream client here; the
Go daemon (`cmd/afepd`) is the networked implementation.
