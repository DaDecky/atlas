package consumer

import (
	"testing"
	"time"
)

func TestBrokerAndHTTPLimitsOverride(t *testing.T) {
	t.Setenv("AMQP_URL", "amqp://example:example@localhost:5672/")
	t.Setenv("BROKER_RECONNECT_DELAY", "250ms")
	t.Setenv("HTTP_READ_TIMEOUT", "9s")

	c, err := ConfigFromEnv()
	if err != nil || c.BrokerReconnectDelay != 250*time.Millisecond || c.HTTPReadTimeout != 9*time.Second ||
		c.BrokerHeartbeat != 10*time.Second {
		t.Fatalf("configured=%+v err=%v", c, err)
	}

	t.Setenv("BROKER_RECONNECT_DELAY", "1ms")

	if _, err = ConfigFromEnv(); err == nil {
		t.Fatal("out of range delay accepted")
	}
}

func TestQueueConfiguration(t *testing.T) {
	t.Setenv("AMQP_URL", "amqp://example:example@localhost:5672/")

	for _, name := range []string{
		"CONSUMER_PREFETCH",
		"CONSUMER_QUEUE_MAX_LENGTH",
		"CONSUMER_QUEUE_MAX_BYTES",
		"CONSUMER_MESSAGE_TTL",
	} {
		t.Setenv(name, "")
	}

	c, err := ConfigFromEnv()
	if err != nil || c.Prefetch != 1 || c.QueueMaxLength != 10000 || c.QueueMaxBytes != 64<<20 ||
		c.MessageTTL != time.Hour {
		t.Fatalf("defaults=%+v err=%v", c, err)
	}

	t.Setenv("CONSUMER_PREFETCH", "4")
	t.Setenv("CONSUMER_QUEUE_MAX_LENGTH", "120")
	t.Setenv("CONSUMER_QUEUE_MAX_BYTES", "4096")
	t.Setenv("CONSUMER_MESSAGE_TTL", "15m")

	c, err = ConfigFromEnv()
	if err != nil || c.Prefetch != 4 || c.queueArguments()["x-max-length"] != int64(120) ||
		c.queueArguments()["x-message-ttl"] != int64(900000) || c.queueArguments()["x-overflow"] != "reject-publish" {
		t.Fatalf("configured=%+v err=%v", c, err)
	}

	for _, name := range []string{
		"CONSUMER_PREFETCH",
		"CONSUMER_QUEUE_MAX_LENGTH",
		"CONSUMER_QUEUE_MAX_BYTES",
		"CONSUMER_MESSAGE_TTL",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "0")

			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("zero limit accepted")
			}
		})
	}
}

func testConfig(url, name, queue, statePath string) Config {
	return Config{
		AMQPURL:        url,
		Name:           name,
		Queue:          queue,
		StatePath:      statePath,
		Prefetch:       defaultPrefetch,
		QueueMaxLength: defaultQueueMaxLength,
		QueueMaxBytes:  defaultQueueMaxBytes,
		MessageTTL:     defaultMessageTTL,
	}
}
