package broadcast_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/broadcast"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/infrastructure/registry"
)

func testCfg() config.Config {
	cfg := config.Default()
	cfg.WorkerCount = 4
	cfg.SubscriberBufferCap = 8
	return cfg
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	metrics, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range metrics {
		if m.GetName() != name {
			continue
		}
		if len(m.GetMetric()) == 0 {
			return 0
		}
		return m.GetMetric()[0].GetCounter().GetValue()
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

func TestBroadcasterSlowSubscriberDoesNotBlockFast(t *testing.T) {
	promReg := prometheus.NewRegistry()
	cfg := testCfg()
	cfg.SubscriberBufferCap = 1

	reg, err := registry.New(cfg, promReg)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := broadcast.New(cfg, reg, promReg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan domain.SignalEvent, 32)
	go func() {
		_ = bc.Run(ctx, events)
	}()

	ts := time.Now().UTC()
	if err := reg.Register(ctx, "slow"); err != nil {
		t.Fatal(err)
	}
	slowOut, ok := reg.Outbound("slow")
	if !ok {
		t.Fatal("slow outbound missing")
	}
	<-slowOut

	fastCount := 16
	var fastReceived atomic.Int32
	for i := 0; i < fastCount; i++ {
		id := fmt.Sprintf("fast-%d", i)
		if err := reg.Register(ctx, id); err != nil {
			t.Fatal(err)
		}
		out, ok := reg.Outbound(id)
		if !ok {
			t.Fatalf("outbound missing for %s", id)
		}
		go func(ch <-chan []byte) {
			for range ch {
				fastReceived.Add(1)
			}
		}(out)
	}

	const batches = 24
	for i := 0; i < batches; i++ {
		events <- domain.SignalEvent{
			ID:         fmt.Sprintf("evt-%d", i),
			Type:       domain.EventTypeSignal,
			Payload:    []byte("payload"),
			ReceivedAt: ts,
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for fastReceived.Load() < int32(fastCount*batches) {
		if time.Now().After(deadline) {
			t.Fatalf("fast received %d, want %d", fastReceived.Load(), fastCount*batches)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := counterValue(t, promReg, "subscriber_buffer_full_total"); got < 1 {
		t.Fatalf("buffer full counter = %v, want >= 1", got)
	}
	if _, ok := reg.Outbound("slow"); ok {
		t.Fatal("slow subscriber should be removed after buffer full")
	}
}

func TestBroadcasterWorkersShutdown(t *testing.T) {
	promReg := prometheus.NewRegistry()
	cfg := testCfg()
	reg, err := registry.New(cfg, promReg)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := broadcast.New(cfg, reg, promReg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan domain.SignalEvent)
	done := make(chan error, 1)
	go func() { done <- bc.Run(ctx, events) }()

	cancel()
	close(events)

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Run() = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broadcaster shutdown")
	}
}

func BenchmarkFanout1KSubscribers(b *testing.B) {
	promReg := prometheus.NewRegistry()
	cfg := testCfg()
	cfg.WorkerCount = runtimeWorkers()
	cfg.SubscriberBufferCap = 64

	reg, err := registry.New(cfg, promReg)
	if err != nil {
		b.Fatal(err)
	}
	bc, err := broadcast.New(cfg, reg, promReg)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan domain.SignalEvent, 1)
	go func() { _ = bc.Run(ctx, events) }()

	const subs = 1000
	for i := 0; i < subs; i++ {
		id := fmt.Sprintf("sub-%d", i)
		if err := reg.Register(ctx, id); err != nil {
			b.Fatal(err)
		}
		out, ok := reg.Outbound(id)
		if !ok {
			b.Fatal("outbound missing")
		}
		go func(ch <-chan []byte) {
			for range ch {
			}
		}(out)
	}

	ts := time.Now().UTC()
	event := domain.SignalEvent{
		ID: "bench", Type: domain.EventTypeSignal,
		Payload: []byte("payload"), ReceivedAt: ts,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events <- event
	}
}

func runtimeWorkers() int {
	cfg := config.Default()
	return cfg.WorkerCount
}
