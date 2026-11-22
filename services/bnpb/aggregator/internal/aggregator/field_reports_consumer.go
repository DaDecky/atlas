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
	OnApplied func(context.Context, time.Time)
}

const (
	FieldReportsQueue = "field.reports"
	// FieldReportsDeadQueue keeps reports that can never apply, so one bad message
	// cannot block the queue behind it and is still kept for inspection.
	FieldReportsDeadQueue = "field.reports.dead"
)

var errReportContract = errors.New("report delivery contract violation")

// fieldReportsArgs must match the Client API's declaration, or RabbitMQ refuses the second one.
var fieldReportsArgs = amqp.Table{"x-max-length": int32(10000), "x-max-length-bytes": int32(64 << 20), "x-overflow": "reject-publish"}

func DeclareFieldReports(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(FieldReportsQueue, true, false, false, false, fieldReportsArgs)
	return err
}

func declareDeadReports(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(FieldReportsDeadQueue, true, false, false, false, fieldReportsArgs)
	return err
}

// deadLetter copies a delivery to the dead queue and waits for the broker's confirm,
// so the original is acknowledged only once its copy is durable.
func deadLetter(ctx context.Context, ch *amqp.Channel, d amqp.Delivery, reason string) error {
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", FieldReportsDeadQueue, true, false, amqp.Publishing{
		Headers: amqp.Table{"x-dead-reason": reason}, ContentType: d.ContentType, DeliveryMode: amqp.Persistent,
		CorrelationId: d.CorrelationId, MessageId: d.MessageId, Timestamp: d.Timestamp, Type: d.Type, Body: d.Body,
	})
	if err != nil {
		return err
	}

	ok, err := confirm.WaitContext(ctx)
	if err == nil && !ok {
		err = errors.New("dead report not confirmed")
	}

	return err
}

func (c *FieldReportsConsumer) Process(ctx context.Context, delivery amqp.Delivery) error {
	report, err := DecodeFieldReport(delivery.Body)
	if err != nil || report.ReportID != delivery.MessageId || delivery.Type != "FieldReport" {
		return errReportContract
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

	dead, err := conn.Channel()
	if err != nil {
		return err
	}

	if err = declareDeadReports(dead); err != nil {
		return err
	}

	if err = dead.Confirm(false); err != nil {
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

			if errors.Is(err, errReportContract) {
				// A contract violation never heals, so requeueing it would block every report behind it.
				call, cancel = context.WithTimeout(ctx, 10*time.Second)
				err = deadLetter(call, dead, delivery, err.Error())

				cancel()

				if err != nil {
					_ = delivery.Nack(false, true)
					return err
				}

				c.Log.Warn("field_report_dead_lettered", "report_id", delivery.MessageId, "correlation_id", delivery.CorrelationId, "queue", FieldReportsDeadQueue)

				if err = delivery.Ack(false); err != nil {
					return err
				}

				continue
			}

			if err != nil {
				// Store failures are transient: requeue keeps the report until a holder is back.
				_ = delivery.Nack(false, true)
				return err
			}

			if err = delivery.Ack(false); err != nil {
				return err
			}

			if c.OnApplied != nil {
				c.OnApplied(ctx, delivery.Timestamp)
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
