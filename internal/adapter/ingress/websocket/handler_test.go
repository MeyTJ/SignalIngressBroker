package websocket_test

import (
	"context"
	"encoding/json"
	"runtime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	wsingress "SignalIngressBroker/internal/adapter/ingress/websocket"
	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/infrastructure/normalizer"
	"SignalIngressBroker/internal/usecase"
)

func marshalWire(id, typ string, payload []byte, receivedAt time.Time) ([]byte, error) {
	return json.Marshal(struct {
		ID                 string `json:"id"`
		Type               string `json:"type"`
		Payload            []byte `json:"payload"`
		ReceivedAtUnixNano int64  `json:"received_at_unix_nano"`
	}{
		ID:                 id,
		Type:               typ,
		Payload:            payload,
		ReceivedAtUnixNano: receivedAt.UnixNano(),
	})
}

func wirePayload(t *testing.T, id, typ string, payload []byte, receivedAt time.Time) []byte {
	t.Helper()
	body, err := marshalWire(id, typ, payload, receivedAt)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func startTestHandler(t *testing.T, cap int) (*wsingress.Handler, chan domain.SignalEvent, *httptest.Server, config.Config) {
	t.Helper()

	cfg := config.Default()
	cfg.IngressCap = cap
	cfg.ReadBufferSize = 4096
	cfg.WebSocketPath = "/ingress/ws"

	events := make(chan domain.SignalEvent, cfg.IngressCap)
	ingest := usecase.NewIngest(normalizer.NewJSON(), events)
	handler := wsingress.NewHandler(cfg, ingest, nil)

	mux := http.NewServeMux()
	mux.HandleFunc(cfg.WebSocketPath, func(w http.ResponseWriter, r *http.Request) {
		handler.ServeWSForTest(r.Context(), w, r)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return handler, events, server, cfg
}

func TestWebSocketIngestSuccess(t *testing.T) {
	t.Parallel()

	handler, events, server, cfg := startTestHandler(t, 4)
	_ = handler

	url := "ws" + strings.TrimPrefix(server.URL, "http") + cfg.WebSocketPath
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ts := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	msg := wirePayload(t, "evt-1", string(domain.EventTypeSignal), []byte("tick"), ts)
	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		t.Fatal(err)
	}

	select {
	case evt := <-events:
		if evt.ID != "evt-1" {
			t.Fatalf("id = %q", evt.ID)
		}
		if evt.Type != domain.EventTypeSignal {
			t.Fatalf("type = %q", evt.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestWebSocketIngestDropsWhenChannelFull(t *testing.T) {
	t.Parallel()

	handler, _, server, cfg := startTestHandler(t, 0)
	url := "ws" + strings.TrimPrefix(server.URL, "http") + cfg.WebSocketPath
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ts := time.Now().UTC()
	msg := wirePayload(t, "evt-drop", string(domain.EventTypeSignal), []byte("x"), ts)
	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for handler.DroppedIngress() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("expected drop metric")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWebSocketIngestInvalidPayloadContinues(t *testing.T) {
	t.Parallel()

	_, events, server, cfg := startTestHandler(t, 2)
	url := "ws" + strings.TrimPrefix(server.URL, "http") + cfg.WebSocketPath
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("{")); err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	valid := wirePayload(t, "evt-2", string(domain.EventTypeTick), []byte("p"), ts)
	if err := conn.WriteMessage(websocket.TextMessage, valid); err != nil {
		t.Fatal(err)
	}

	select {
	case evt := <-events:
		if evt.ID != "evt-2" {
			t.Fatalf("id = %q", evt.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for valid event after invalid frame")
	}
}

func BenchmarkProcessPayload(b *testing.B) {
	cfg := config.Default()
	events := make(chan domain.SignalEvent, cfg.IngressCap)
	ingest := usecase.NewIngest(normalizer.NewJSON(), events)
	handler := wsingress.NewHandler(cfg, ingest, nil)

	ts := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	payload, err := marshalWire("evt-bench", string(domain.EventTypeSignal), []byte("payload"), ts)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := handler.ProcessPayloadForTest(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadWebSocket(b *testing.B) {
	cfg := config.Default()
	events := make(chan domain.SignalEvent, cfg.IngressCap)
	ingest := usecase.NewIngest(normalizer.NewJSON(), events)
	handler := wsingress.NewHandler(cfg, ingest, nil)

	mux := http.NewServeMux()
	mux.HandleFunc(cfg.WebSocketPath, func(w http.ResponseWriter, r *http.Request) {
		handler.ServeWSForTest(context.Background(), w, r)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http") + cfg.WebSocketPath
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	go func() {
		for range events {
		}
	}()

	ts := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	msg, err := marshalWire("evt-read", string(domain.EventTypeSignal), []byte("payload"), ts)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := handler.ProcessedIngress()
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			b.Fatal(err)
		}
		for handler.ProcessedIngress() == before {
			runtime.Gosched()
		}
	}
}
