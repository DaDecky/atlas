package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Health struct{ ready atomic.Bool }

type alert struct {
	HazardID   string    `json:"hazard_id"`
	Revision   int64     `json:"revision"`
	Severity   string    `json:"severity"`
	AreaName   string    `json:"area_name"`
	IngestedAt time.Time `json:"ingested_at"`
}

// queueArgs is empty, so no broker limit ever drops a critical alert.
var queueArgs amqp.Table

func (h *Health) Ready() bool { return h.ready.Load() }

func (h *Health) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(
				w,
				http.StatusMethodNotAllowed,
				map[string]any{"error": map[string]string{"code": "method_not_allowed"}},
			)

			return
		}

		if !h.Ready() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	state, err := OpenProcessedState(cfg.StatePath)
	if err != nil {
		return err
	}

	health := &Health{}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/", health.Handler())
	server := &http.Server{
		Addr:              cfg.HealthAddr,
		Handler:           mux,
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
	}

	listener, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil {
		return err
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	if cfg.MetricsPushURL != "" {
		go metrics.push(runCtx, cfg.MetricsPushURL, 5*time.Second, log)
	}

	serverDone := make(chan error, 1)

	go func() {
		serverDone <- server.Serve(listener)

		stop()
	}()

	for runCtx.Err() == nil {
		err := consumeOnce(runCtx, cfg, state, health, log)
		if runCtx.Err() != nil {
			break
		}

		if err != nil {
			log.Warn("consumer_disconnected", "queue", queue, "reason", err.Error())
		}

		timer := time.NewTimer(cfg.BrokerReconnectDelay)
		select {
		case <-runCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	shutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	_ = server.Shutdown(shutdown)

	serveErr := <-serverDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}

	return serveErr
}

func consumeOnce(ctx context.Context, cfg Config, state *ProcessedState, health *Health, log *slog.Logger) error {
	conn, deliveries, err := subscribe(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer conn.Close()

	health.ready.Store(true)
	metrics.brokerConnected.Store(true)

	defer func() {
		health.ready.Store(false)
		metrics.brokerConnected.Store(false)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("broker delivery stream closed")
			}

			if err := handle(state, delivery, time.Now(), log); err != nil {
				return err
			}
		}
	}
}

func subscribe(ctx context.Context, cfg Config, log *slog.Logger) (*amqp.Connection, <-chan amqp.Delivery, error) {
	started := time.Now()
	dial := func(network, address string) (net.Conn, error) {
		return (&net.Dialer{Timeout: cfg.BrokerDialTimeout}).DialContext(ctx, network, address)
	}

	conn, err := amqp.DialConfig(cfg.AMQPURL, amqp.Config{Heartbeat: cfg.BrokerHeartbeat, Dial: dial})
	if err != nil {
		log.Warn("broker_connect_failed", "dependency", "rabbitmq", "latency_ms", time.Since(started).Milliseconds())
		return nil, nil, err
	}

	deliveries, err := declareAndConsume(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	log.Info("broker_connected", "queue", queue, "dependency", "rabbitmq", "latency_ms", time.Since(started).Milliseconds())

	return conn, deliveries, nil
}

func declareAndConsume(conn *amqp.Connection, cfg Config) (<-chan amqp.Delivery, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if _, err = ch.QueueDeclare(queue, true, false, false, false, queueArgs); err != nil {
		return nil, err
	}

	if err = ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return nil, err
	}

	return ch.Consume(queue, name, false, false, false, false, nil)
}

// handle records the alert before acknowledging it, so a crash between the two causes a duplicate, not a loss.
func handle(state *ProcessedState, delivery amqp.Delivery, received time.Time, log *slog.Logger) error {
	var message alert

	err := json.Unmarshal(delivery.Body, &message)
	if err != nil || !validHazardID(message.HazardID) || message.Revision < 1 {
		log.Warn("alert_rejected",
			"queue", queue,
			"correlation_id", delivery.CorrelationId,
			"hazard_id", message.HazardID,
			"revision", message.Revision,
			"reason", "invalid_message",
		)
		metrics.rejected.Add(1)

		return delivery.Nack(false, false)
	}

	delivered, err := state.RecordRevision(message.HazardID, message.Revision)
	if err != nil {
		return err
	}

	if !delivered {
		log.Info("duplicate_alert", "hazard_id", message.HazardID, "revision", message.Revision, "correlation_id", delivery.CorrelationId)
		metrics.duplicate.Add(1)

		return delivery.Ack(false)
	}

	log.Info("field_alert_delivered",
		"hazard_id", message.HazardID,
		"revision", message.Revision,
		"severity", message.Severity,
		"area_name", message.AreaName,
		"correlation_id", delivery.CorrelationId,
		"latency_ms", received.Sub(message.IngestedAt).Milliseconds(),
	)
	metrics.delivery(received.Sub(message.IngestedAt))

	return delivery.Ack(false)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
