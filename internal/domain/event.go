package domain

import (
	"bytes"
	"time"
)

type EventType string

const (
	EventTypeSignal EventType = "signal"
	EventTypeTick   EventType = "tick"
	EventTypeQuote  EventType = "quote"
)

func (t EventType) Valid() bool {
	switch t {
	case EventTypeSignal, EventTypeTick, EventTypeQuote:
		return true
	default:
		return false
	}
}

type SignalEvent struct {
	ID         string
	Type       EventType
	Payload    []byte
	ReceivedAt time.Time
}

func (e SignalEvent) Valid() bool {
	return e.Validate() == nil
}

func (e SignalEvent) Validate() error {
	if e.ID == "" {
		return ErrInvalidPayload
	}
	if !e.Type.Valid() {
		return ErrInvalidPayload
	}
	if len(e.Payload) == 0 {
		return ErrInvalidPayload
	}
	if e.ReceivedAt.IsZero() {
		return ErrInvalidPayload
	}
	return nil
}

func (e SignalEvent) Equal(other SignalEvent) bool {
	return e.ID == other.ID &&
		e.Type == other.Type &&
		bytes.Equal(e.Payload, other.Payload) &&
		e.ReceivedAt.Equal(other.ReceivedAt)
}
