.PHONY: build test lint bench clean

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
