# SignalIngressBroker

SignalIngressBroker is a high-throughput, zero-allocation data gateway engineered in Go. It manages thousands of concurrent WebSocket and gRPC connections, normalizing raw external feeds into strictly typed domain events. Utilizing advanced goroutine pooling, it efficiently broadcasts real-time payloads to isolated downstream microservices at scale.

## Architecture

Clean Architecture with dependencies pointing inward. The composition root lives in `cmd/broker`; all wiring happens there.

```
cmd/broker/                 composition root (main only)
api/
  proto/                    gRPC contracts (downstream boundary)
  openapi/                  HTTP operational contracts (placeholder)
internal/
  domain/                   SignalEvent, domain errors (no I/O)
  usecase/                  ports, Ingest, Broadcast orchestration
  adapter/
    ingress/websocket/      WebSocket ingress adapter
    ingress/grpc/           gRPC client-stream ingress (IngestSignals)
    egress/                 subscriber write adapter
  infrastructure/
    config/                 configuration defaults
    normalizer/             EventNormalizer implementation (stub)
    registry/               sharded in-memory SubscriberRegistry
    broadcast/              worker-pool Broadcaster with drop-on-full
```

| Layer | Package | Role |
|-------|---------|------|
| Domain | `internal/domain` | Entities and errors |
| Use case | `internal/usecase` | Ports and application services |
| Adapter | `internal/adapter/...` | Protocol-facing handlers |
| Infrastructure | `internal/infrastructure/...` | Registry, broadcast pool, config |
| API | `api/proto`, `api/openapi` | External contracts |

Current scaffold exposes stubs only; hot-path pooling, sharded registry, and drop-on-full backpressure are implemented in later phases.

## Build

```bash
make proto   # regenerate api/gen from api/proto
make build
make test
make lint
make bench
```

Or without Make:

```bash
go build -o bin/broker ./cmd/broker
go test -race ./...
```
