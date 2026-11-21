package aggregator

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// dialBroker connects with a socket deadline of timeout, so a broker that accepts TCP but
// never answers cannot hold the caller. The caller owns the socket and clears the deadline.
func dialBroker(ctx context.Context, url string, heartbeat, timeout time.Duration) (*amqp.Connection, net.Conn, error) {
	var socket net.Conn

	dial := func(network, address string) (net.Conn, error) {
		s, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}

		socket = s
		_ = s.SetDeadline(time.Now().Add(timeout))

		return s, nil
	}

	conn, err := amqp.DialConfig(url, amqp.Config{Heartbeat: heartbeat, Dial: dial})
	if err != nil {
		if socket != nil {
			_ = socket.Close()
		}

		return nil, nil, err
	}

	return conn, socket, nil
}
