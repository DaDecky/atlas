package notifier

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	AMQPURL, StatePath, HealthAddr string
	MetricsPushURL                 string
	Prefetch                       int

	HTTPReadHeaderTimeout, HTTPReadTimeout, HTTPWriteTimeout, HTTPIdleTimeout, ShutdownTimeout time.Duration
	BrokerDialTimeout, BrokerHeartbeat, BrokerReconnectDelay                                   time.Duration
}

const (
	name = "notifier"
	// The Aggregator declares this queue with no arguments, and a second declaration must match it.
	queue = "field.alerts"

	defaultStatePath  = "/data/delivered-alerts.txt"
	defaultHealthAddr = ":8093"
	defaultPrefetch   = 1

	defaultHTTPReadHeaderTimeout = 2 * time.Second
	defaultHTTPReadTimeout       = 3 * time.Second
	defaultHTTPWriteTimeout      = 3 * time.Second
	defaultHTTPIdleTimeout       = 15 * time.Second
	defaultShutdownTimeout       = 3 * time.Second
	defaultBrokerDialTimeout     = 3 * time.Second
	defaultBrokerHeartbeat       = 10 * time.Second
	defaultBrokerReconnectDelay  = time.Second
)

func ConfigFromEnv() (Config, error) {
	c := Config{
		AMQPURL:    os.Getenv("AMQP_URL"),
		StatePath:  os.Getenv("CONSUMER_STATE_PATH"),
		HealthAddr: os.Getenv("CONSUMER_HEALTH_ADDR"),
		// Optional: a VictoriaMetrics /api/v1/import/prometheus URL.
		MetricsPushURL: os.Getenv("METRICS_PUSH_URL"),
		Prefetch:       defaultPrefetch,
	}

	if c.AMQPURL == "" {
		return c, errors.New("AMQP_URL is required")
	}

	if c.StatePath == "" {
		c.StatePath = defaultStatePath
	}

	if c.HealthAddr == "" {
		c.HealthAddr = defaultHealthAddr
	}

	if raw := os.Getenv("CONSUMER_PREFETCH"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 1024 {
			return c, errors.New("invalid CONSUMER_PREFETCH")
		}

		c.Prefetch = value
	}

	durations := []struct {
		key                string
		fallback, min, max time.Duration
		dest               *time.Duration
	}{
		{"HTTP_READ_HEADER_TIMEOUT", defaultHTTPReadHeaderTimeout, 100 * time.Millisecond, time.Minute, &c.HTTPReadHeaderTimeout},
		{"HTTP_READ_TIMEOUT", defaultHTTPReadTimeout, 100 * time.Millisecond, 5 * time.Minute, &c.HTTPReadTimeout},
		{"HTTP_WRITE_TIMEOUT", defaultHTTPWriteTimeout, 100 * time.Millisecond, 5 * time.Minute, &c.HTTPWriteTimeout},
		{"HTTP_IDLE_TIMEOUT", defaultHTTPIdleTimeout, time.Second, 10 * time.Minute, &c.HTTPIdleTimeout},
		{"SHUTDOWN_TIMEOUT", defaultShutdownTimeout, time.Second, 2 * time.Minute, &c.ShutdownTimeout},
		{"BROKER_DIAL_TIMEOUT", defaultBrokerDialTimeout, 100 * time.Millisecond, time.Minute, &c.BrokerDialTimeout},
		{"BROKER_HEARTBEAT", defaultBrokerHeartbeat, time.Second, 5 * time.Minute, &c.BrokerHeartbeat},
		{"BROKER_RECONNECT_DELAY", defaultBrokerReconnectDelay, 100 * time.Millisecond, time.Minute, &c.BrokerReconnectDelay},
	}
	for _, d := range durations {
		value := d.fallback

		if raw := os.Getenv(d.key); raw != "" {
			parsed, err := time.ParseDuration(raw)
			if err != nil || parsed < d.min || parsed > d.max {
				return c, fmt.Errorf("invalid %s", d.key)
			}

			value = parsed
		}

		*d.dest = value
	}

	return c, nil
}
