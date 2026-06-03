.PHONY: build test lint bench clean proto

PROTOC ?= protoc
PROTO_DIR := api/proto

proto:
	$(PROTOC) -I $(PROTO_DIR) \
		--go_out=. --go_opt=module=SignalIngressBroker \
		--go-grpc_out=. --go-grpc_opt=module=SignalIngressBroker \
		$(PROTO_DIR)/signalingress/v1/broker.proto

BINARY := broker
CMD := ./cmd/broker

build:
	go build -o bin/$(BINARY) $(CMD)

test:
	go test -race ./...

lint:
	golangci-lint run ./...

bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf bin/
