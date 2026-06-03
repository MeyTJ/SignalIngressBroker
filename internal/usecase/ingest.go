package usecase

import (
	"context"

	"SignalIngressBroker/internal/domain"
)

type Ingest struct {
	normalizer EventNormalizer
	events     chan<- domain.SignalEvent
}

func NewIngest(normalizer EventNormalizer, events chan<- domain.SignalEvent) *Ingest {
	return &Ingest{
		normalizer: normalizer,
		events:     events,
	}
}

func (uc *Ingest) Handle(ctx context.Context, raw []byte) error {
	_ = ctx
	event, err := uc.normalizer.Normalize(raw)
	if err != nil {
		return err
	}
	if err := event.Validate(); err != nil {
		return err
	}
	select {
	case uc.events <- event:
		return nil
	default:
		return domain.ErrIngressSaturated
	}
}
