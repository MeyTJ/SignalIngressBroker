package usecase

import (
	"context"

	"SignalIngressBroker/internal/domain"
)

// EventNormalizer maps raw ingress bytes to a validated domain event.
type EventNormalizer interface {
	Normalize(raw []byte) (domain.SignalEvent, error)
}

// SubscriberTarget is a fan-out destination with a bounded outbound buffer.
type SubscriberTarget struct {
	ID       string
	Outbound chan []byte
	Close    func()
}

// SubscriberRegistry tracks in-process subscriber sessions (no external cache).
type SubscriberRegistry interface {
	Register(ctx context.Context, subscriberID string) error
	Unregister(ctx context.Context, subscriberID string) error
	Get(ctx context.Context, subscriberID string) error
	SnapshotActive(ctx context.Context) ([]string, error)
	SnapshotTargets(ctx context.Context) ([]SubscriberTarget, error)
}

// Broadcaster fans out normalized events to active subscribers with backpressure.
type Broadcaster interface {
	Run(ctx context.Context, events <-chan domain.SignalEvent) error
}

// IngressHandler serves an ingress protocol until context cancellation.
type IngressHandler interface {
	Serve(ctx context.Context) error
}

// EgressWriter delivers payloads to a single subscriber connection.
type EgressWriter interface {
	Write(ctx context.Context, subscriberID string, payload []byte) error
}
