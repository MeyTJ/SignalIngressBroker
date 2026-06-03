package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/usecase"
)

type stubNormalizer struct {
	event domain.SignalEvent
	err   error
}

func (s stubNormalizer) Normalize([]byte) (domain.SignalEvent, error) {
	return s.event, s.err
}

func TestIngestHandle(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	valid := domain.SignalEvent{
		ID: "1", Type: domain.EventTypeSignal,
		Payload: []byte("p"), ReceivedAt: ts,
	}

	tests := []struct {
		name      string
		normalizer stubNormalizer
		cap       int
		wantErr   error
	}{
		{
			name:       "success",
			normalizer: stubNormalizer{event: valid},
			cap:        1,
			wantErr:    nil,
		},
		{
			name:       "normalize error",
			normalizer: stubNormalizer{err: domain.ErrInvalidPayload},
			cap:        1,
			wantErr:    domain.ErrInvalidPayload,
		},
		{
			name: "invalid event after normalize",
			normalizer: stubNormalizer{event: domain.SignalEvent{ID: "1"}},
			cap:        1,
			wantErr:    domain.ErrInvalidPayload,
		},
		{
			name:       "ingress saturated",
			normalizer: stubNormalizer{event: valid},
			cap:        0,
			wantErr:    domain.ErrIngressSaturated,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ch := make(chan domain.SignalEvent, tt.cap)
			uc := usecase.NewIngest(tt.normalizer, ch)
			err := uc.Handle(context.Background(), []byte("raw"))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Handle() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
