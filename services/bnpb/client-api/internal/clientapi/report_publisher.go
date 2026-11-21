package clientapi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitReports struct {
	URL     string
	Timeout time.Duration
}

const FieldReportsQueue = "field.reports"

// fieldReportsArgs must match the Aggregator's declaration, or RabbitMQ refuses the second one.
var fieldReportsArgs = amqp.Table{"x-max-length": int32(10000), "x-max-length-bytes": int32(64 << 20), "x-overflow": "reject-publish"}

// Each bounded call owns its AMQP channel. Concurrent HTTP requests never share
// confirms or socket deadlines, and no downstream consumer is required for 202.
func (p *RabbitReports) PublishReport(ctx context.Context, report FieldReport, id string) error {
	ctx, cancel := context.WithTimeout(ctx, p.Timeout)
	defer cancel()

	if p.URL == "" {
		return errors.New("report broker unconfigured")
	}

	var socket net.Conn

	conn, err := amqp.DialConfig(p.URL, amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, address string) (net.Conn, error) {
		var dialErr error

		socket, dialErr = (&net.Dialer{}).DialContext(ctx, network, address)
		if dialErr == nil {
			deadline, _ := ctx.Deadline()
			_ = socket.SetDeadline(deadline)
		}

		return socket, dialErr
	}})

	if socket != nil {
		defer socket.Close()
	}

	if err != nil {
		return errors.New("report broker unavailable")
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return errors.New("report channel unavailable")
	}

	if _, err = ch.QueueDeclare(FieldReportsQueue, true, false, false, false, fieldReportsArgs); err != nil {
		return errors.New("report queue unavailable")
	}

	if err = ch.Confirm(false); err != nil {
		return errors.New("report confirms unavailable")
	}

	returned := ch.NotifyReturn(make(chan amqp.Return, 1))

	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}

	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", FieldReportsQueue, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", Type: "FieldReport", MessageId: report.ReportID, CorrelationId: id, Timestamp: time.Now().UTC(), Body: raw})
	if err != nil || confirm == nil {
		return errors.New("report publication unconfirmed")
	}

	ack, err := confirm.WaitContext(ctx)
	if err != nil || !ack {
		return errors.New("report publication unconfirmed")
	}

	select {
	case <-returned:
		return errors.New("report publication unroutable")
	default:
		return nil
	}
}
