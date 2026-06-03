package domain

import "errors"

var (
	ErrInvalidPayload  = errors.New("invalid payload")
	ErrSubscriberGone  = errors.New("subscriber gone")
	ErrIngressSaturated = errors.New("ingress saturated")
)
