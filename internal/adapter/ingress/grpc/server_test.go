package grpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	signalingressv1 "SignalIngressBroker/api/gen/signalingress/v1"
	grpcingress "SignalIngressBroker/internal/adapter/ingress/grpc"
	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/infrastructure/normalizer"
	"SignalIngressBroker/internal/usecase"
)

const bufConnSize = 1024 * 1024

func startBufconnServer(t *testing.T, cap int) (*grpcingress.Server, chan domain.SignalEvent, signalingressv1.SignalIngressBrokerClient, func()) {
	t.Helper()

	cfg := config.Default()
	cfg.IngressCap = cap
	cfg.GRPCListenAddr = "passthrough:///bufnet"

	events := make(chan domain.SignalEvent, cfg.IngressCap)
	ingest := usecase.NewIngest(normalizer.NewJSON(), events)
	server := grpcingress.NewServer(cfg, ingest, nil)

	lis := bufconn.Listen(bufConnSize)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.ServeListener(ctx, lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}

	cleanup := func() {
		_ = conn.Close()
		cancel()
		<-done
	}

	return server, events, signalingressv1.NewSignalIngressBrokerClient(conn), cleanup
}

func TestIngestSignalsIntegration(t *testing.T) {
	t.Parallel()

	server, events, client, cleanup := startBufconnServer(t, 4)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.IngestSignals(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if err := stream.Send(&signalingressv1.SignalEnvelope{
			Id:                 "evt-grpc",
			Type:               string(domain.EventTypeSignal),
			Payload:            []byte("payload"),
			ReceivedAtUnixNano: ts.UnixNano(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAccepted() != 3 {
		t.Fatalf("accepted = %d, want 3", resp.GetAccepted())
	}
	if server.ProcessedIngress() != 3 {
		t.Fatalf("processed = %d, want 3", server.ProcessedIngress())
	}

	received := 0
	deadline := time.Now().Add(2 * time.Second)
	for received < 3 {
		select {
		case evt := <-events:
			if evt.ID != "evt-grpc" {
				t.Fatalf("id = %q", evt.ID)
			}
			received++
		case <-time.After(time.Until(deadline)):
			t.Fatalf("received %d events, want 3", received)
		}
	}
}

func TestIngestSignalsDropsWhenIngressFull(t *testing.T) {
	t.Parallel()

	_, events, client, cleanup := startBufconnServer(t, 0)
	defer cleanup()
	_ = events

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.IngestSignals(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Now().UTC()
	if err := stream.Send(&signalingressv1.SignalEnvelope{
		Id: "evt-drop", Type: string(domain.EventTypeSignal),
		Payload: []byte("x"), ReceivedAtUnixNano: ts.UnixNano(),
	}); err != nil {
		t.Fatal(err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetDropped() != 1 {
		t.Fatalf("dropped = %d, want 1", resp.GetDropped())
	}
}

func TestIngestSignalsInvalidEnvelope(t *testing.T) {
	t.Parallel()

	_, _, client, cleanup := startBufconnServer(t, 4)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.IngestSignals(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Send(&signalingressv1.SignalEnvelope{Id: "bad"}); err != nil {
		t.Fatal(err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetInvalid() != 1 {
		t.Fatalf("invalid = %d, want 1", resp.GetInvalid())
	}
}
