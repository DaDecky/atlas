package aggregator

import (
	"context"
	"errors"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FieldReportsConsumer struct {
	URL       string
	Heartbeat time.Duration
	Store     FieldReportStore
	Log       *slog.Logger
}

const FieldReportsQueue = "field.reports"

func DeclareFieldReports(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(FieldReportsQueue, true, false, false, false, amqp.Table{"x-max-length": int32(10000), "x-max-length-bytes": int32(64 << 20), "x-overflow": "reject-publish"})
	return err
}

func (c *FieldReportsConsumer) Process(ctx context.Context, delivery amqp.Delivery) error {
	report, err := DecodeFieldReport(delivery.Body)
	if err != nil || report.ReportID != delivery.MessageId || delivery.Type != "FieldReport" {
		return errors.New("report delivery contract violation")
	}

	_, _, err = c.Store.ApplyFieldReport(ctx, report)

	return err
}

func (c *FieldReportsConsumer) session(ctx context.Context) error {
	conn, socket, err := dialBroker(ctx, c.URL, c.Heartbeat, 5*time.Second)
	if err != nil {
		return errors.New("report broker unavailable")
	}
	defer socket.Close()
	defer conn.Close()

	stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
	defer stop()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	if err = DeclareFieldReports(ch); err != nil {
		return err
	}

	if err = ch.Qos(1, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.Consume(FieldReportsQueue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	_ = socket.SetDeadline(time.Time{})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("report consumer disconnected")
			}

			call, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = c.Process(call, delivery)

			cancel()

			if err != nil {
				// Requeue keeps every report. Only the Client API publishes here, under the same contract.
				_ = delivery.Nack(false, true)
				return err
			}

			if err = delivery.Ack(false); err != nil {
				return err
			}

			c.Log.Info("field_report_applied", "report_id", delivery.MessageId, "correlation_id", delivery.CorrelationId)
		}
	}
}

func (c *FieldReportsConsumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := c.session(ctx); err != nil && ctx.Err() == nil {
			c.Log.Warn("field_reports_retry", "reason", err.Error())
		}

		if !pause(ctx, time.Second) {
			return
		}
	}
}
