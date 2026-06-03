package normalizer

import (
	"encoding/json"
	"sync"
	"time"

	"SignalIngressBroker/internal/domain"
)

type wireEvent struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	Payload            []byte `json:"payload"`
	ReceivedAtUnixNano int64  `json:"received_at_unix_nano"`
}

type JSON struct {
	decodePool sync.Pool
}

func NewJSON() *JSON {
	j := &JSON{}
	j.decodePool.New = func() any {
		return &wireEvent{}
	}
	return j
}

func (j *JSON) Normalize(raw []byte) (domain.SignalEvent, error) {
	w := j.decodePool.Get().(*wireEvent)
	defer func() {
		*w = wireEvent{}
		j.decodePool.Put(w)
	}()

	if err := json.Unmarshal(raw, w); err != nil {
		return domain.SignalEvent{}, domain.ErrInvalidPayload
	}

	receivedAt := time.Unix(0, w.ReceivedAtUnixNano)
	if w.ReceivedAtUnixNano == 0 {
		receivedAt = time.Now()
	}

	event := domain.SignalEvent{
		ID:         w.ID,
		Type:       domain.EventType(w.Type),
		Payload:    append([]byte(nil), w.Payload...),
		ReceivedAt: receivedAt,
	}
	if err := event.Validate(); err != nil {
		return domain.SignalEvent{}, err
	}
	return event, nil
}
