package usecase

import (
	"context"

	"SignalIngressBroker/internal/domain"
)

type Broadcast struct {
	registry    SubscriberRegistry
	broadcaster Broadcaster
}

func NewBroadcast(registry SubscriberRegistry, broadcaster Broadcaster) *Broadcast {
	return &Broadcast{
		registry:    registry,
		broadcaster: broadcaster,
	}
}

func (uc *Broadcast) Run(ctx context.Context, events <-chan domain.SignalEvent) error {
	_ = uc.registry
	return uc.broadcaster.Run(ctx, events)
}
