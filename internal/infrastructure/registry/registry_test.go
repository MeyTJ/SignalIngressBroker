package registry_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/infrastructure/registry"
)

func newTestRegistry(t *testing.T, shards int) *registry.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	cfg := config.Default()
	cfg.ShardCount = shards
	r, err := registry.New(cfg, reg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRegistryRegisterUnregisterGet(t *testing.T) {
	t.Parallel()

	r := newTestRegistry(t, 256)
	ctx := context.Background()

	if err := r.Register(ctx, "sub-a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, "sub-a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(ctx, "sub-a"); err != nil {
		t.Fatal("duplicate register should be idempotent")
	}

	ids, err := r.SnapshotActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "sub-a" {
		t.Fatalf("snapshot = %v", ids)
	}

	if err := r.Unregister(ctx, "sub-a"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(r.Get(ctx, "sub-a"), domain.ErrSubscriberGone) {
		t.Fatal("expected subscriber gone")
	}
	if !errors.Is(r.Unregister(ctx, "sub-a"), domain.ErrSubscriberGone) {
		t.Fatal("expected subscriber gone on double unregister")
	}
	if r.ActiveCount() != 0 {
		t.Fatalf("active = %d", r.ActiveCount())
	}
}

func TestRegistryInvalidID(t *testing.T) {
	t.Parallel()

	r := newTestRegistry(t, 256)
	ctx := context.Background()

	if !errors.Is(r.Register(ctx, ""), domain.ErrInvalidPayload) {
		t.Fatal("expected invalid payload on empty id")
	}
}

func TestRegistryRace(t *testing.T) {
	r := newTestRegistry(t, 256)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		i := i
		id := fmt.Sprintf("sub-%d", i%80)

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Register(ctx, id)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Unregister(ctx, id)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = r.SnapshotActive(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Get(ctx, id)
		}()
	}
	wg.Wait()
}

func TestRegistryPrometheusGauge(t *testing.T) {
	t.Parallel()

	promReg := prometheus.NewRegistry()
	cfg := config.Default()
	cfg.ShardCount = 256
	r, err := registry.New(cfg, promReg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := r.Register(ctx, "sub-1"); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(ctx, "sub-2"); err != nil {
		t.Fatal(err)
	}

	metrics, err := promReg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, m := range metrics {
		if m.GetName() == "active_subscribers" {
			found = true
			if got := m.GetMetric()[0].GetGauge().GetValue(); got != 2 {
				t.Fatalf("gauge = %v, want 2", got)
			}
		}
	}
	if !found {
		t.Fatal("active_subscribers metric not found")
	}
}

func TestRegistryInvalidShardCount(t *testing.T) {
	t.Parallel()

	_, err := registry.New(config.Config{ShardCount: 300}, prometheus.NewRegistry())
	if err == nil {
		t.Fatal("expected error for non-power-of-two shard count")
	}
}
