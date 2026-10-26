# Contributing to AFEP

AFEP is **contract-first**: `proto/afep/v1/` is the source of truth.
Code follows the contract, never the other way around.

## Development setup

```bash
# Go toolchain + buf codegen plugins
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Python reference
python3 -m venv .venv && .venv/bin/pip install -e ./python

# Local broker for integration tests (optional)
docker compose up -d
```

## Test commands

```bash
make lint            # buf lint + go vet
make test            # go test ./...
make python-test     # python -m unittest discover (needs deps)
go build ./... && ./bin/afepd -h   # daemon smoke

# infra-gated integration (needs the local broker):
AFEP_NATS=nats://127.0.0.1:4222 go test ./internal/bus/nats -run Integration -v

# two-site federation (optional):
docker compose -f deploy/federation/compose.yml up -d
AFEP_NATS=nats://127.0.0.1:4224 go test ./internal/bus/nats -run Integration -v
```

Platform: Linux-first (the daemon targets POSIX). CI runs on Ubuntu.

## PR checklist

- [ ] `buf lint` clean and generated stubs committed (`buf generate`,
      plus `--template buf.gen.python.yaml` when schemas changed).
- [ ] `gofmt`, `go vet`, `go test ./...` green; python
      `unittest discover` green.
- [ ] **Schema changes are cross-language**: update every SDK, regenerate
      BOTH fixture sides in the same PR —

      ```bash
      AFEP_FIXTURES=1 go test ./internal/interop -run GenerateFixtures
      AFEP_FIXTURES=1 .venv/bin/python -m unittest tests.test_interop
      ```

- [ ] New behavior has tests; env-gated tests must skip cleanly without
      infrastructure (`AFEP_NATS` unset).
- [ ] CHANGELOG.md gained a section; docs/ARCHITECTURE.md residuals
      updated if a gap closed or opened.
- [ ] No key material, tokens or absolute personal paths in code/tests.

## Design rules

- Never sign for an identity whose key you do not hold.
- Every envelope ingress validates the payload schema; poison messages
  are Terminated, not redelivered forever.
- Deterministic protobuf serialization everywhere signatures are
  computed (Go `MarshalOptions{Deterministic: true}` /
  Python `SerializeToString(deterministic=True)`).
- Keep honest gaps honest: document residuals instead of overselling.
