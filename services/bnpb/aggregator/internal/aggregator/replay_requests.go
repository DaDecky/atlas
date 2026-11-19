package aggregator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ReplayRequest struct {
	Source     string `json:"source"`
	Limit      int    `json:"limit"`
	ReasonCode string `json:"reason_code,omitempty"`
}

// ReplayRequests stores commands independently of the source holder. The holder
// calls Serve while it owns the lease and binds Apply to its fenced writer epoch.
type ReplayRequests struct {
	URL       string
	Heartbeat time.Duration
}

func (r ReplayRequest) validReason() bool {
	return r.ReasonCode == "" || replayReasonCode.MatchString(r.ReasonCode)
}

func (r ReplayRequest) valid() bool {
	return validSource(r.Source) && r.Limit >= 1 && r.Limit <= 1000 && r.validReason()
}

func (r ReplayRequests) channel(ctx context.Context, source string, run func(*amqp.Channel, net.Conn, string) error) error {
	var socket net.Conn

	conn, err := amqp.DialConfig(r.URL, amqp.Config{Heartbeat: r.Heartbeat, Dial: func(network, address string) (net.Conn, error) {
		var dialErr error

		socket, dialErr = (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, address)
		if dialErr == nil {
			_ = socket.SetDeadline(time.Now().Add(2 * time.Second))
		}

		return socket, dialErr
	}})

	if socket != nil {
		defer socket.Close()
	}

	if err != nil {
		return errors.New("replay broker unavailable")
	}

	stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
	defer stop()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	name := "ops.replay." + strings.ToLower(source)
	args := amqp.Table{"x-single-active-consumer": true, "x-max-length": int32(1000), "x-overflow": "reject-publish"}

	if _, err = ch.QueueDeclare(name, true, false, false, false, args); err != nil {
		return err
	}

	return run(ch, socket, name)
}

func (r ReplayRequests) Enqueue(ctx context.Context, request ReplayRequest) (string, error) {
	if !request.valid() {
		return "", errors.New("invalid replay request")
	}

	var token [16]byte

	_, _ = rand.Read(token[:])
	id := hex.EncodeToString(token[:])
	body, _ := json.Marshal(request)

	err := r.channel(ctx, request.Source, func(ch *amqp.Channel, _ net.Conn, queue string) error {
		if err := ch.Confirm(false); err != nil {
			return err
		}

		returned := ch.NotifyReturn(make(chan amqp.Return, 1))
		msg := amqp.Publishing{MessageId: id, Type: "ReplayRequest", DeliveryMode: amqp.Persistent, ContentType: "application/json", Body: body}

		confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, msg)
		if err != nil || confirm == nil {
			return errors.New("replay request not published")
		}

		ack, err := confirm.WaitContext(ctx)
		if err != nil || !ack {
			return errors.New("replay request not confirmed")
		}

		select {
		case <-returned:
			return errors.New("replay request not routed")
		default:
			return nil
		}
	})

	return id, err
}

// Serve processes one source's requests until the holder's context ends.
// Connection loss leaves unacknowledged commands available to the next holder.
func (r ReplayRequests) Serve(ctx context.Context, source string, apply ReplaySource) error {
	if !validSource(source) || apply == nil {
		return errors.New("invalid replay worker")
	}

	return r.channel(ctx, source, func(ch *amqp.Channel, socket net.Conn, queue string) error {
		if err := ch.Qos(1, 0, false); err != nil {
			return err
		}

		deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			return err
		}

		_ = socket.SetDeadline(time.Time{})

		return consumeReplayRequests(ctx, deliveries, source, apply)
	})
}

func consumeReplayRequests(ctx context.Context, deliveries <-chan amqp.Delivery, source string, apply ReplaySource) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("replay consumer disconnected")
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			var request ReplayRequest

			if delivery.Type != "ReplayRequest" || len(delivery.Body) > 4096 ||
				json.Unmarshal(delivery.Body, &request) != nil || request.Source != source || !request.valid() {
				if err := delivery.Reject(false); err != nil {
					return err
				}

				continue
			}

			call, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := apply(call, request.Limit, request.ReasonCode)

			cancel()

			if err != nil {
				return err // Closing the consumer requeues the unacknowledged request.
			}

			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}
