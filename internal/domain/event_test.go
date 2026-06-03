package domain_test

import (
	"errors"
	"testing"
	"time"

	"SignalIngressBroker/internal/domain"
)

func TestSignalEventValidate(t *testing.T) {
	t.Parallel()

	receivedAt := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	base := domain.SignalEvent{
		ID:         "evt-1",
		Type:       domain.EventTypeSignal,
		Payload:    []byte(`{"p":1}`),
		ReceivedAt: receivedAt,
	}

	tests := []struct {
		name    string
		event   domain.SignalEvent
		wantErr error
	}{
		{
			name:    "valid signal",
			event:   base,
			wantErr: nil,
		},
		{
			name: "valid tick",
			event: domain.SignalEvent{
				ID: "evt-2", Type: domain.EventTypeTick,
				Payload: []byte("t"), ReceivedAt: receivedAt,
			},
			wantErr: nil,
		},
		{
			name: "valid quote",
			event: domain.SignalEvent{
				ID: "evt-3", Type: domain.EventTypeQuote,
				Payload: []byte("q"), ReceivedAt: receivedAt,
			},
			wantErr: nil,
		},
		{
			name:    "missing id",
			event:   func() domain.SignalEvent { e := base; e.ID = ""; return e }(),
			wantErr: domain.ErrInvalidPayload,
		},
		{
			name:    "unknown type",
			event:   func() domain.SignalEvent { e := base; e.Type = domain.EventType("unknown"); return e }(),
			wantErr: domain.ErrInvalidPayload,
		},
		{
			name:    "empty type",
			event:   func() domain.SignalEvent { e := base; e.Type = ""; return e }(),
			wantErr: domain.ErrInvalidPayload,
		},
		{
			name:    "empty payload",
			event:   func() domain.SignalEvent { e := base; e.Payload = nil; return e }(),
			wantErr: domain.ErrInvalidPayload,
		},
		{
			name:    "zero received at",
			event:   func() domain.SignalEvent { e := base; e.ReceivedAt = time.Time{}; return e }(),
			wantErr: domain.ErrInvalidPayload,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.event.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
			if (err == nil) != tt.event.Valid() {
				t.Fatalf("Valid() = %v, want %v", tt.event.Valid(), err == nil)
			}
		})
	}
}

func TestEventTypeValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		typ   domain.EventType
		valid bool
	}{
		{name: "signal", typ: domain.EventTypeSignal, valid: true},
		{name: "tick", typ: domain.EventTypeTick, valid: true},
		{name: "quote", typ: domain.EventTypeQuote, valid: true},
		{name: "empty", typ: "", valid: false},
		{name: "unknown", typ: domain.EventType("heartbeat"), valid: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.typ.Valid(); got != tt.valid {
				t.Fatalf("Valid() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestSignalEventEqual(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	left := domain.SignalEvent{
		ID: "evt-1", Type: domain.EventTypeSignal,
		Payload: []byte("abc"), ReceivedAt: ts,
	}
	right := domain.SignalEvent{
		ID: "evt-1", Type: domain.EventTypeSignal,
		Payload: []byte("abc"), ReceivedAt: ts,
	}

	tests := []struct {
		name   string
		a      domain.SignalEvent
		b      domain.SignalEvent
		equal  bool
	}{
		{name: "identical", a: left, b: right, equal: true},
		{name: "same value different payload slice", a: left, b: func() domain.SignalEvent {
			e := right
			e.Payload = append([]byte(nil), right.Payload...)
			return e
		}(), equal: true},
		{
			name: "different id",
			a: left,
			b: func() domain.SignalEvent { e := right; e.ID = "evt-2"; return e }(),
			equal: false,
		},
		{
			name: "different type",
			a: left,
			b: func() domain.SignalEvent { e := right; e.Type = domain.EventTypeTick; return e }(),
			equal: false,
		},
		{
			name: "different payload",
			a: left,
			b: func() domain.SignalEvent { e := right; e.Payload = []byte("xyz"); return e }(),
			equal: false,
		},
		{
			name: "different received at",
			a: left,
			b: func() domain.SignalEvent {
				e := right
				e.ReceivedAt = ts.Add(time.Second)
				return e
			}(),
			equal: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.a.Equal(tt.b); got != tt.equal {
				t.Fatalf("Equal() = %v, want %v", got, tt.equal)
			}
			if got := tt.b.Equal(tt.a); got != tt.equal {
				t.Fatalf("symmetric Equal() = %v, want %v", got, tt.equal)
			}
		})
	}
}
