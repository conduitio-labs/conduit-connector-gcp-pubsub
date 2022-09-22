.PHONY: build test

build:
	go build -o conduit-connector-gcp-pubsub cmd/gcppubsub/main.go

test:
	go test $(GOTEST_FLAGS) -count=1 -race ./...

lint:
	golangci-lint run -c .golangci.yml --go=1.18

dep:
	go mod download
	go mod tidy

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go