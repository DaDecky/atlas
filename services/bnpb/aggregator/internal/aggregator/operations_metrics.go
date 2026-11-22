package aggregator

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueStats struct {
	Ready, Consumers int
	Available        bool
}

// ObserveQueues uses passive declarations on a separate, deadline-bound connection.
// A missing queue closes its AMQP channel, so each inspection opens its own.
func ObserveQueues(ctx context.Context, url string, heartbeat time.Duration, observe func(string, QueueStats)) {
	conn, socket, dialErr := dialBroker(ctx, url, heartbeat, 2*time.Second)
	if dialErr == nil {
		defer socket.Close()
		defer conn.Close()

		stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
		defer stop()
	}

	for _, name := range []string{FieldReportsQueue, FieldReportsDeadQueue, fieldAlertsQueue, "hazard.events.consumer-a", "hazard.events.consumer-b"} {
		if ctx.Err() != nil {
			return
		}

		stats := QueueStats{}
		if conn != nil {
			stats = inspectQueue(conn, name)
		}

		observe(name, stats)
	}
}

func inspectQueue(conn *amqp.Connection, name string) QueueStats {
	ch, err := conn.Channel()
	if err != nil {
		return QueueStats{}
	}
	defer ch.Close()

	state, err := ch.QueueDeclarePassive(name, false, false, false, false, nil)
	if err != nil {
		return QueueStats{}
	}

	return QueueStats{Ready: state.Messages, Consumers: state.Consumers, Available: true}
}

func (s *Store) OutboxStats(ctx context.Context) (count, oldest int64, err error) {
	err = s.query.QueryRow(ctx, `SELECT count(*),COALESCE(floor(EXTRACT(EPOCH FROM min(created_at)))::bigint,0) FROM public.publication_outbox`).Scan(&count, &oldest)
	return count, oldest, err
}
