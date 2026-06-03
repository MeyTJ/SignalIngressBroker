package grpc

import (
	"context"

	"SignalIngressBroker/internal/usecase"
)

var _ usecase.IngressHandler = (*Server)(nil)

type Server struct {
	ingest *usecase.Ingest
}

func NewServer(ingest *usecase.Ingest) *Server {
	return &Server{ingest: ingest}
}

func (s *Server) Serve(ctx context.Context) error {
	_ = s.ingest
	<-ctx.Done()
	return ctx.Err()
}
