package grpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	signalingressv1 "SignalIngressBroker/api/gen/signalingress/v1"
	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/infrastructure/normalizer"
	"SignalIngressBroker/internal/usecase"
)

var _ usecase.IngressHandler = (*Server)(nil)
var _ signalingressv1.SignalIngressBrokerServer = (*Server)(nil)

type Server struct {
	signalingressv1.UnimplementedSignalIngressBrokerServer

	cfg       config.Config
	ingest    *usecase.Ingest
	logger    *slog.Logger
	dropped   atomic.Uint64
	processed atomic.Uint64
	invalid   atomic.Uint64

	grpcServer *grpc.Server
}

func NewServer(cfg config.Config, ingest *usecase.Ingest, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfg:    cfg,
		ingest: ingest,
		logger: logger,
	}
}

func (s *Server) DroppedIngress() uint64  { return s.dropped.Load() }
func (s *Server) ProcessedIngress() uint64 { return s.processed.Load() }
func (s *Server) InvalidIngress() uint64  { return s.invalid.Load() }

func (s *Server) Register(grpcServer *grpc.Server) {
	signalingressv1.RegisterSignalIngressBrokerServer(grpcServer, s)
}

func (s *Server) Serve(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.cfg.GRPCListenAddr)
	if err != nil {
		return err
	}
	return s.ServeListener(ctx, lis)
}

// ServeListener serves gRPC on an existing listener (used in tests with bufconn).
func (s *Server) ServeListener(ctx context.Context, lis net.Listener) error {
	s.grpcServer = grpc.NewServer()
	s.Register(s.grpcServer)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		stopped := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			s.grpcServer.Stop()
		}
		_ = lis.Close()
		<-errCh
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *Server) IngestSignals(stream signalingressv1.SignalIngressBroker_IngestSignalsServer) error {
	ctx := stream.Context()
	var accepted, dropped, invalid uint64

	for {
		envelope, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&signalingressv1.IngestSignalsResponse{
				Accepted: accepted,
				Dropped:  dropped,
				Invalid:  invalid,
			})
		}
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.Canceled {
				return err
			}
			return err
		}

		switch s.processEnvelope(ctx, envelope) {
		case ingestAccepted:
			accepted++
		case ingestDropped:
			dropped++
		case ingestInvalid:
			invalid++
		}
	}
}

type ingestOutcome int

const (
	ingestAccepted ingestOutcome = iota
	ingestDropped
	ingestInvalid
)

func (s *Server) processEnvelope(ctx context.Context, envelope *signalingressv1.SignalEnvelope) ingestOutcome {
	if envelope == nil {
		s.invalid.Add(1)
		return ingestInvalid
	}
	if len(envelope.Payload) > s.cfg.MaxMessageBytes {
		s.invalid.Add(1)
		return ingestInvalid
	}

	raw, err := normalizer.MarshalWire(
		envelope.GetId(),
		envelope.GetType(),
		envelope.GetPayload(),
		envelope.GetReceivedAtUnixNano(),
	)
	if err != nil {
		s.invalid.Add(1)
		return ingestInvalid
	}

	err = s.ingest.Handle(ctx, raw)
	if errors.Is(err, domain.ErrIngressSaturated) {
		s.dropped.Add(1)
		return ingestDropped
	}
	if err != nil {
		if errors.Is(err, domain.ErrInvalidPayload) {
			s.invalid.Add(1)
			return ingestInvalid
		}
		return ingestInvalid
	}
	s.processed.Add(1)
	return ingestAccepted
}
