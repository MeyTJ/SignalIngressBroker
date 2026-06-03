package broadcast

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/usecase"
)

var _ usecase.Broadcaster = (*Broadcaster)(nil)

type Broadcaster struct {
	registry   usecase.SubscriberRegistry
	workers    int
	bufferFull prometheus.Counter
}

func New(cfg config.Config, registry usecase.SubscriberRegistry, registerer prometheus.Registerer) (*Broadcaster, error) {
	if registry == nil {
		return nil, errors.New("broadcast: registry is required")
	}
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "subscriber_buffer_full_total",
		Help: "Total subscriber connections dropped because the outbound buffer was full.",
	})
	if err := registerer.Register(counter); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			return nil, err
		}
		counter = already.ExistingCollector.(prometheus.Counter)
	}

	workers := cfg.WorkerCount
	if workers <= 0 {
		workers = runtime.NumCPU() * 2
		if workers < 1 {
			workers = 1
		}
	}

	return &Broadcaster{
		registry:   registry,
		workers:    workers,
		bufferFull: counter,
	}, nil
}

func (b *Broadcaster) Run(ctx context.Context, events <-chan domain.SignalEvent) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(b.workers)
	for i := 0; i < b.workers; i++ {
		go func() {
			defer wg.Done()
			b.worker(ctx, events)
		}()
	}

	wg.Wait()
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (b *Broadcaster) worker(ctx context.Context, events <-chan domain.SignalEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			b.fanOut(ctx, event)
		}
	}
}

func (b *Broadcaster) fanOut(ctx context.Context, event domain.SignalEvent) {
	targets, err := b.registry.SnapshotTargets(ctx)
	if err != nil || len(targets) == 0 {
		return
	}

	for _, target := range targets {
		payload := append([]byte(nil), event.Payload...)
		b.deliver(ctx, target, payload)
	}
}

func (b *Broadcaster) deliver(ctx context.Context, target usecase.SubscriberTarget, payload []byte) {
	_ = ctx
	select {
	case target.Outbound <- payload:
		return
	default:
		b.bufferFull.Add(1)
		if target.Close != nil {
			target.Close()
		}
	}
}
