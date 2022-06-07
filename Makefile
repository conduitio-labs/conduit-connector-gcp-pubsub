.PHONY: build test

build:
	go build -o conduit-connector-gcp-pubsub cmd/gcp-pubsub/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run -c .golangci.yml --go=1.18

dep:
	go mod download
	go mod tidy