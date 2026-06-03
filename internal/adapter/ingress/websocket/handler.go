package websocket

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"SignalIngressBroker/internal/domain"
	"SignalIngressBroker/internal/infrastructure/bufferpool"
	"SignalIngressBroker/internal/infrastructure/config"
	"SignalIngressBroker/internal/usecase"
)

var _ usecase.IngressHandler = (*Handler)(nil)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Handler struct {
	cfg        config.Config
	ingest     *usecase.Ingest
	logger     *slog.Logger
	readPool   *bufferpool.Pool
	dropped    atomic.Uint64
	processed  atomic.Uint64
	httpServer *http.Server
}

func NewHandler(cfg config.Config, ingest *usecase.Ingest, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		cfg:      cfg,
		ingest:   ingest,
		logger:   logger,
		readPool: bufferpool.New(cfg.ReadBufferSize),
	}
}

func (h *Handler) DroppedIngress() uint64 {
	return h.dropped.Load()
}

func (h *Handler) ProcessedIngress() uint64 {
	return h.processed.Load()
}

func (h *Handler) Serve(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc(h.cfg.WebSocketPath, func(w http.ResponseWriter, r *http.Request) {
		h.serveWS(ctx, w, r)
	})

	h.httpServer = &http.Server{
		Addr:              h.cfg.HTTPListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- h.httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = h.httpServer.Shutdown(shutdownCtx)
		<-errCh
		return ctx.Err()
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func (h *Handler) serveWS(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", "err", err)
		return
	}
	defer conn.Close()

	conn.SetReadLimit(int64(h.cfg.MaxMessageBytes))

	for {
		if err := h.readAndProcess(ctx, conn); err != nil {
			if errors.Is(err, context.Canceled) || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			if errors.Is(err, io.EOF) {
				return
			}
			h.logger.Debug("websocket read ended", "err", err)
			return
		}
	}
}

func (h *Handler) readAndProcess(ctx context.Context, conn *websocket.Conn) error {
	buf := h.readPool.get()
	defer h.readPool.put(buf)

	_, reader, err := conn.NextReader()
	if err != nil {
		return err
	}

	buf = buf[:0]
	for {
		n, err := reader.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if len(buf) > h.cfg.MaxMessageBytes {
			_, _ = io.Copy(io.Discard, reader)
			return domain.ErrInvalidPayload
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(buf) == cap(buf) {
			_, _ = io.Copy(io.Discard, reader)
			return domain.ErrInvalidPayload
		}
	}

	// buf is reused after Handle returns; normalizer copies payload into domain.SignalEvent.
	return h.processPayload(ctx, buf)
}

// ServeWSForTest exposes the WebSocket handler for httptest wiring.
func (h *Handler) ServeWSForTest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	h.serveWS(ctx, w, r)
}

// ProcessPayloadForTest runs the ingest hot path without a live connection.
func (h *Handler) ProcessPayloadForTest(ctx context.Context, payload []byte) error {
	return h.processPayload(ctx, payload)
}

// ReadOneForTest reads and processes a single WebSocket frame.
func (h *Handler) ReadOneForTest(ctx context.Context, conn *websocket.Conn) error {
	return h.readAndProcess(ctx, conn)
}

func (h *Handler) processPayload(ctx context.Context, payload []byte) error {
	err := h.ingest.Handle(ctx, payload)
	if errors.Is(err, domain.ErrIngressSaturated) {
		h.dropped.Add(1)
		return nil
	}
	if err != nil {
		if errors.Is(err, domain.ErrInvalidPayload) {
			return nil
		}
		return err
	}
	h.processed.Add(1)
	return nil
}
