package domain_test

import (
	"errors"
	"fmt"
	"testing"

	"SignalIngressBroker/internal/domain"
)

func TestDomainErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "invalid payload", err: domain.ErrInvalidPayload},
		{name: "subscriber gone", err: domain.ErrSubscriberGone},
		{name: "ingress saturated", err: domain.ErrIngressSaturated},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			wrapped := fmt.Errorf("wrap: %w", tt.err)
			if !errors.Is(wrapped, tt.err) {
				t.Fatalf("errors.Is(wrapped, err) = false")
			}
		})
	}
}
