package broadcast

import (
	"context"

	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/usecase"
)

var _ usecase.Broadcaster = (*Stub)(nil)

type Stub struct{}

func NewStub() *Stub {
	return &Stub{}
}

func (s *Stub) Run(ctx context.Context, events <-chan domain.SignalEvent) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-events:
			if !ok {
				return nil
			}
		}
	}
}
