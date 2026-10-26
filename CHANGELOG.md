# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-10-25

### Added

- Protobuf contracts (`afep.v1`): AgentCard, TaskEnvelope with the
  PROBE/BID/COMMIT/RUNNING/DONE/FAILED/ESCALATED lifecycle, AgentTransport
  service with bi-directional streaming.
- Go gRPC service scaffold (`internal/transport/grpcsvc`).
- NATS JetStream bus adapter with subject scheme, at-least-once publish and
  KV-backed card bucket (`internal/bus/nats`).
- ed25519 identity: card issue/sign/verify (`internal/identity`).
- Task lifecycle state machine with cross-domain escalation rules
  (`internal/lifecycle`).
- buf lint/codegen, CI workflow, dev compose for JetStream.
