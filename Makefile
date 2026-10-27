.PHONY: all fmt lint test build gen nats-up nats-down demo fuzz bench conformance

all: lint test build

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './gen/*')

lint:
	buf lint
	go vet ./...

test:
	go test -race ./...

python-test:
	cd python && python3 -m unittest discover -s tests -v

build:
	go build -o bin/afepd ./cmd/afepd

fuzz:
	go test -run='^$$' -fuzz=FuzzParseSubject -fuzztime=10s ./internal/bus/nats/
	go test -run='^$$' -fuzz=FuzzTaskstoreOpen -fuzztime=10s ./internal/taskstore/
	go test -run='^$$' -fuzz=FuzzEnvelopeBytes -fuzztime=10s ./internal/identity/

bench:
	go test -run='^$$' -bench=. -benchtime=100x ./internal/identity/ ./internal/mesh/ ./internal/taskstore/

conformance:
	cd python && PYTHONPATH=. .venv/bin/python ../conformance/verify.py

gen:
	buf generate

nats-up:
	docker compose up -d

nats-down:
	docker compose down

demo:
	go run ./cmd/afepd
