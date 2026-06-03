package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"SignalIngressBroker/internal/adapter/ingress/grpc"
	"SignalIngressBroker/internal/adapter/ingress/websocket"
	"SignalIngressBroker/internal/domain"
	infraBroadcast "SignalIngressBroker/internal/infrastructure/broadcast"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/infrastructure/normalizer"
	"SignalIngressBroker/internal/infrastructure/registry"
	"SignalIngressBroker/internal/usecase"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Default()

	events := make(chan domain.SignalEvent, cfg.IngressCap)
	eventNormalizer := normalizer.NewJSON()
	subscriberRegistry, err := registry.New(cfg, nil)
	if err != nil {
		logger.Error("registry init failed", "err", err)
		os.Exit(1)
	}
	broadcaster, err := infraBroadcast.New(cfg, subscriberRegistry, nil)
	if err != nil {
		logger.Error("broadcaster init failed", "err", err)
		os.Exit(1)
	}

	ingestUC := usecase.NewIngest(eventNormalizer, events)
	broadcastUC := usecase.NewBroadcast(subscriberRegistry, broadcaster)

	wsHandler := websocket.NewHandler(cfg, ingestUC, logger)
	grpcServer := grpc.NewServer(ingestUC)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("broker starting",
		"http", cfg.HTTPListenAddr,
		"grpc", cfg.GRPCListenAddr,
	)

	errCh := make(chan error, 2)
	go func() { errCh <- wsHandler.Serve(ctx) }()
	go func() { errCh <- grpcServer.Serve(ctx) }()
	go func() { errCh <- broadcastUC.Run(ctx, events) }()

	select {
	case <-ctx.Done():
		logger.Info("broker shutting down", "reason", ctx.Err())
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			logger.Error("broker stopped with error", "err", err)
			os.Exit(1)
		}
	}
}
