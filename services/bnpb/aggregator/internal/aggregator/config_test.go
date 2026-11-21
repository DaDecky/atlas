package aggregator

import (
	"context"
	"net"
	"testing"
	"time"
)

func validTestEnv(t *testing.T) {
	t.Helper()

	for key, value := range map[string]string{
		"DATABASE_URL":              "postgres://example:example@localhost/canonical",
		"RABBITMQ_URL":              "amqp://example:example@localhost/",
		"BMKG_BASE_URL":             "http://bmkg:8081",
		"PVMBG_BASE_URL":            "http://pvmbg:8082",
		"BMKG_API_KEY":              "test-bmkg",
		"PVMBG_TOKEN":               "test-pvmbg",
		"AGGREGATOR_INTERNAL_TOKEN": "test-internal",
	} {
		t.Setenv(key, value)
	}

	for _, key := range []string{
		"HTTP_ADDR",
		"POLL_INTERVAL",
		"SOURCE_TIMEOUT",
		"RETRY_BACKOFF",
		"QUERY_TIMEOUT",
		"HAZARD_READ_CACHE_TTL",
		"PUBLISH_TIMEOUT",
		"SOURCE_RETRIES",
		"QUERY_CAPACITY",
		"QUERY_POOL_SIZE",
		"INGEST_POOL_SIZE",
		"HTTP_READ_HEADER_TIMEOUT",
		"HTTP_READ_TIMEOUT",
		"HTTP_IDLE_TIMEOUT",
		"HTTP_MAX_HEADER_BYTES",
		"STARTUP_TIMEOUT",
		"SHUTDOWN_TIMEOUT",
		"DB_CONNECT_TIMEOUT",
		"SOURCE_IDLE_CONN_TIMEOUT",
		"FAILURE_MARK_TIMEOUT",
		"WARNING_LOOKBACK",
		"BROKER_HEARTBEAT",
		"MAX_SOURCE_RECORDS",
		"MAX_SOURCE_BYTES",
	} {
		t.Setenv(key, "")
	}
}

func TestConfigLimitsOverrideAndBounds(t *testing.T) {
	validTestEnv(t)

	t.Setenv("MAX_SOURCE_BYTES", "131072")
	t.Setenv("WARNING_LOOKBACK", "2h")
	t.Setenv("HTTP_READ_TIMEOUT", "7s")

	c, err := ConfigFromEnv()
	if err != nil || c.MaxSourceBytes != 131072 || c.WarningLookback != 2*time.Hour || c.HTTPReadTimeout != 7*time.Second {
		t.Fatal(c, err)
	}

	for key, value := range map[string]string{"MAX_SOURCE_BYTES": "10", "WARNING_LOOKBACK": "48h", "STARTUP_TIMEOUT": "0s"} {
		t.Setenv(key, value)

		if _, err = ConfigFromEnv(); err == nil {
			t.Fatal(key, "accepted")
		}

		t.Setenv(key, "")
	}
}

func TestConfigDefaultsAndRejection(t *testing.T) {
	validTestEnv(t)

	c, err := ConfigFromEnv()
	if err != nil || c.SourceTimeout != 4*time.Second || c.PollInterval != 3*time.Second || c.Address != ":8083" {
		t.Fatal(c, err)
	}

	for _, tc := range []struct{ key, value string }{
		{"BMKG_API_KEY", ""},
		{"PVMBG_TOKEN", "test-bmkg"},
		{"AGGREGATOR_INTERNAL_TOKEN", "test-pvmbg"},
		{"SOURCE_RETRIES", "6"},
		{"QUERY_CAPACITY", "0"},
		{"QUERY_POOL_SIZE", "0"},
		{"SOURCE_TIMEOUT", "0s"},
		{"POLL_INTERVAL", "bad"},
		{"BMKG_BASE_URL", "http://username:password@bmkg:8081"},
		{"PVMBG_BASE_URL", "http://pvmbg:8082?token=secret"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv(tc.key, tc.value)

			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestPublisherBoundsUnresponsiveBrokerHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	done := make(chan struct{})
	stop := make(chan struct{})

	go func() {
		defer close(done)

		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		<-stop
	}()

	p := &RabbitPublisher{URL: "amqp://test:test@" + listener.Addr().String() + "/", Timeout: 50 * time.Millisecond}
	defer p.Close()

	start := time.Now()
	err = p.Publish(context.Background(), Publication{Event: HazardEvent{HazardID: "bounded"}})

	close(stop)
	<-done

	if err == nil || time.Since(start) > time.Second {
		t.Fatal("unresponsive broker was not bounded", err)
	}
}

func TestConfigAllowsZeroRetriesButNotZeroCapacity(t *testing.T) {
	validTestEnv(t)
	t.Setenv("SOURCE_RETRIES", "0")

	c, err := ConfigFromEnv()
	if err != nil || c.SourceRetries != 0 {
		t.Fatal(c, err)
	}

	t.Setenv("QUERY_CAPACITY", "0")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("zero query capacity accepted")
	}
}
