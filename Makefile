.PHONY: all fmt lint test build gen nats-up nats-down demo

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

gen:
	buf generate

nats-up:
	docker compose up -d

nats-down:
	docker compose down

demo:
	go run ./cmd/afepd
