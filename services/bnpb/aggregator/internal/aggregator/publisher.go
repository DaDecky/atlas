package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Called by a single worker. No network waits occur in a poller or read handler.
type RabbitPublisher struct {
	URL       string
	Timeout   time.Duration
	Heartbeat time.Duration
	Log       *slog.Logger
	conn      *amqp.Connection
	channel   *amqp.Channel
	socket    net.Conn
}

type EventPublisher interface {
	Publish(context.Context, Publication) error
	PublishCritical(context.Context, Publication) error
	Close()
}

const Exchange = "hazard.events"

func (p *RabbitPublisher) Close() {
	if p.socket != nil {
		_ = p.socket.Close()
	}

	p.conn = nil
	p.channel = nil
	p.socket = nil
}

func (p *RabbitPublisher) connect(ctx context.Context) error {
	if p.conn != nil && !p.conn.IsClosed() {
		return nil
	}

	p.Close()

	conn, socket, err := dialBroker(ctx, p.URL, p.Heartbeat, p.Timeout)
	if err != nil {
		return errors.New("broker connection failed")
	}

	p.socket = socket
	p.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		p.Close()
		return errors.New("broker channel failed")
	}

	p.channel = ch
	if err = ch.ExchangeDeclare(Exchange, "fanout", true, false, false, false, nil); err != nil {
		p.Close()
		return errors.New("broker exchange failed")
	}

	if err = ch.Confirm(false); err != nil {
		p.Close()
		return errors.New("broker confirm setup failed")
	}

	return nil
}

func (p *RabbitPublisher) Publish(ctx context.Context, item Publication) error {
	return p.publish(ctx, item, Exchange, "")
}

func (p *RabbitPublisher) PublishCritical(ctx context.Context, item Publication) error {
	return p.publish(ctx, item, "", "field.alerts")
}

func (p *RabbitPublisher) publish(ctx context.Context, item Publication, exchange, key string) (err error) {
	start := time.Now()

	defer func() {
		if p.Log != nil {
			p.Log.Info("broker_call",
				"correlation_id", item.CorrelationID,
				"hazard_id", item.Event.HazardID,
				"revision", item.Event.Revision,
				"source", item.Event.Source,
				"dependency", "rabbitmq",
				"exchange", exchange,
				"routing_key", key,
				"latency_ms", time.Since(start).Milliseconds(),
				"confirmed", err == nil)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, p.Timeout)
	defer cancel()

	if err := p.connect(ctx); err != nil {
		return err
	}

	deadline, _ := ctx.Deadline()
	_ = p.socket.SetDeadline(deadline)

	raw, err := json.Marshal(item.Event)
	if err != nil {
		return err
	}
	// Socket deadlines bound AMQP writes and RPCs as well as confirmation waits.
	message := amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		ContentType:   "application/json",
		Type:          "HazardEvent",
		MessageId:     fmt.Sprintf("%s:%d", item.Event.HazardID, item.Event.Revision),
		CorrelationId: item.CorrelationID,
		Timestamp:     time.Now().UTC(),
		Body:          raw,
	}

	if key == "field.alerts" {
		if _, err = p.channel.QueueDeclare("field.alerts", true, false, false, false, nil); err != nil {
			p.Close()
			return err
		}
	}

	confirm, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, false, false, message)
	if err == nil && confirm != nil {
		var ack bool

		ack, err = confirm.WaitContext(ctx)
		if err == nil && !ack {
			err = errors.New("broker nack")
		}
	} else if err == nil {
		err = errors.New("broker confirm missing")
	}

	if err != nil {
		p.Close()
		return errors.New("publication unconfirmed")
	}

	_ = p.socket.SetDeadline(time.Time{})

	return nil
}
