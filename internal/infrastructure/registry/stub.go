package registry

import (
	"context"

	"SignalIngressBroker/internal/usecase"
)

var _ usecase.SubscriberRegistry = (*Stub)(nil)

type Stub struct{}

func NewStub() *Stub {
	return &Stub{}
}

func (s *Stub) Register(ctx context.Context, subscriberID string) error {
	_, _ = ctx, subscriberID
	return nil
}

func (s *Stub) Unregister(ctx context.Context, subscriberID string) error {
	_, _ = ctx, subscriberID
	return nil
}

func (s *Stub) Get(ctx context.Context, subscriberID string) error {
	_, _ = ctx, subscriberID
	return nil
}

func (s *Stub) SnapshotActive(ctx context.Context) ([]string, error) {
	_ = ctx
	return nil, nil
}

func (s *Stub) SnapshotTargets(ctx context.Context) ([]usecase.SubscriberTarget, error) {
	_ = ctx
	return nil, nil
}
