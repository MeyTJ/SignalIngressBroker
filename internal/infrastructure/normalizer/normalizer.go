package normalizer

import (
	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/usecase"
)

var (
	_ usecase.EventNormalizer = (*Stub)(nil)
	_ usecase.EventNormalizer = (*JSON)(nil)
)

type Stub struct{}

func NewStub() *Stub {
	return &Stub{}
}

func (s *Stub) Normalize(raw []byte) (domain.SignalEvent, error) {
	_ = raw
	return domain.SignalEvent{}, domain.ErrInvalidPayload
}
