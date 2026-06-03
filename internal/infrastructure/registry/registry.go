package registry

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/usecase"
)

var _ usecase.SubscriberRegistry = (*Registry)(nil)

type shard struct {
	mu   sync.RWMutex
	subs map[string]*session
}

type Registry struct {
	shards     []*shard
	mask       uint32
	bufferCap  int
	active     atomic.Int64
	subGauge   prometheus.Gauge
}

func New(cfg config.Config, registerer prometheus.Registerer) (*Registry, error) {
	shardCount, err := normalizeShardCount(cfg.ShardCount)
	if err != nil {
		return nil, err
	}
	bufferCap := cfg.SubscriberBufferCap
	if bufferCap <= 0 {
		bufferCap = 64
	}
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "active_subscribers",
		Help: "Number of active in-process subscriber sessions.",
	})
	if err := registerer.Register(gauge); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			return nil, err
		}
		gauge = alreadyRegistered.ExistingCollector.(prometheus.Gauge)
	}

	shards := make([]*shard, shardCount)
	for i := range shards {
		shards[i] = &shard{subs: make(map[string]*session)}
	}

	return &Registry{
		shards:    shards,
		mask:      uint32(shardCount - 1),
		bufferCap: bufferCap,
		subGauge:  gauge,
	}, nil
}

func (r *Registry) Register(ctx context.Context, subscriberID string) error {
	return r.register(ctx, subscriberID, nil)
}

func (r *Registry) RegisterWithCloseHook(ctx context.Context, subscriberID string, onClose func()) error {
	return r.register(ctx, subscriberID, onClose)
}

func (r *Registry) register(ctx context.Context, subscriberID string, onClose func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if subscriberID == "" {
		return domain.ErrInvalidPayload
	}

	sh := r.shardFor(subscriberID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if existing, exists := sh.subs[subscriberID]; exists {
		if onClose != nil {
			existing.onClose = onClose
		}
		return nil
	}
	sh.subs[subscriberID] = &session{
		outbound: make(chan []byte, r.bufferCap),
		onClose:  onClose,
	}
	r.incActive()
	return nil
}

func (r *Registry) Unregister(ctx context.Context, subscriberID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if subscriberID == "" {
		return domain.ErrInvalidPayload
	}

	sh := r.shardFor(subscriberID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if !r.removeLocked(sh, subscriberID) {
		return domain.ErrSubscriberGone
	}
	return nil
}

func (r *Registry) Get(ctx context.Context, subscriberID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if subscriberID == "" {
		return domain.ErrInvalidPayload
	}

	sh := r.shardFor(subscriberID)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	if _, exists := sh.subs[subscriberID]; !exists {
		return domain.ErrSubscriberGone
	}
	return nil
}

func (r *Registry) SnapshotActive(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	n := int(r.active.Load())
	if n == 0 {
		return nil, nil
	}

	ids := make([]string, 0, n)
	for _, sh := range r.shards {
		sh.mu.RLock()
		for id := range sh.subs {
			ids = append(ids, id)
		}
		sh.mu.RUnlock()
	}
	return ids, nil
}

func (r *Registry) SnapshotTargets(ctx context.Context) ([]usecase.SubscriberTarget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	n := int(r.active.Load())
	if n == 0 {
		return nil, nil
	}

	targets := make([]usecase.SubscriberTarget, 0, n)
	for _, sh := range r.shards {
		sh.mu.RLock()
		for id, sess := range sh.subs {
			targets = append(targets, usecase.SubscriberTarget{
				ID:       id,
				Outbound: sess.outbound,
				Close:    r.closeCallback(id),
			})
		}
		sh.mu.RUnlock()
	}
	return targets, nil
}

func (r *Registry) Outbound(subscriberID string) (<-chan []byte, bool) {
	sh := r.shardFor(subscriberID)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	sess, ok := sh.subs[subscriberID]
	if !ok {
		return nil, false
	}
	return sess.outbound, true
}

func (r *Registry) ActiveCount() int64 {
	return r.active.Load()
}

func (r *Registry) closeCallback(subscriberID string) func() {
	return func() {
		r.closeSession(subscriberID)
	}
}

func (r *Registry) closeSession(subscriberID string) {
	sh := r.shardFor(subscriberID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	sess, ok := sh.subs[subscriberID]
	if !ok {
		return
	}
	r.removeLocked(sh, subscriberID)
	if sess.onClose != nil {
		sess.onClose()
	}
}

func (r *Registry) removeLocked(sh *shard, subscriberID string) bool {
	sess, ok := sh.subs[subscriberID]
	if !ok {
		return false
	}
	delete(sh.subs, subscriberID)
	close(sess.outbound)
	r.decActive()
	return true
}

func (r *Registry) shardFor(subscriberID string) *shard {
	return r.shards[hashSubscriberID(subscriberID, r.mask)]
}

func (r *Registry) incActive() {
	r.subGauge.Set(float64(r.active.Add(1)))
}

func (r *Registry) decActive() {
	r.subGauge.Set(float64(r.active.Add(-1)))
}

func hashSubscriberID(id string, mask uint32) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return h.Sum32() & mask
}

func normalizeShardCount(n int) (int, error) {
	if n <= 0 {
		n = 256
	}
	if n&(n-1) != 0 {
		return 0, errors.New("registry: shard count must be a power of two")
	}
	return n, nil
}
