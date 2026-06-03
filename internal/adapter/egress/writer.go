package egress

import (
	"context"

	"SignalIngressBroker/internal/usecase"
)

var _ usecase.EgressWriter = (*Writer)(nil)

type Writer struct{}

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) Write(ctx context.Context, subscriberID string, payload []byte) error {
	_, _, _ = ctx, subscriberID, payload
	return nil
}
