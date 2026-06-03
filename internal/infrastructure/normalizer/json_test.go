package normalizer_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/normalizer"
)

func TestJSONNormalize(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	raw, err := json.Marshal(struct {
		ID                 string `json:"id"`
		Type               string `json:"type"`
		Payload            []byte `json:"payload"`
		ReceivedAtUnixNano int64  `json:"received_at_unix_nano"`
	}{
		ID: "1", Type: string(domain.EventTypeSignal),
		Payload: []byte("p"), ReceivedAtUnixNano: ts.UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}

	n := normalizer.NewJSON()
	event, err := n.Normalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.ID != "1" {
		t.Fatalf("id = %q", event.ID)
	}

	_, err = n.Normalize([]byte("{"))
	if !errors.Is(err, domain.ErrInvalidPayload) {
		t.Fatalf("err = %v", err)
	}
}
