package aggregator

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL, RabbitMQURL, BMKGURL, PVMBGURL, BMKGKey, PVMBGToken, InternalToken, Address string
	PollInterval, SourceTimeout, Backoff, QueryTimeout, PublishTimeout                       time.Duration
	SourceRetries, PublishRetries, QueryCapacity, PublishCapacity, FieldReservedPercent      int
	IngestPool, QueryPool                                                                    int32

	HTTPReadHeaderTimeout, HTTPReadTimeout, HTTPIdleTimeout, StartupTimeout, ShutdownTimeout time.Duration
	DBConnectTimeout, SourceIdleConnTimeout, FailureMarkTimeout, WarningLookback             time.Duration
	BrokerHeartbeat                                                                          time.Duration
	SensorURL, SensorToken                                                                   string
	SensorInterval, SensorTimeout, SensorStaleAfter                                          time.Duration
	HTTPMaxHeaderBytes, MaxSourceRecords, MaxSourceBytes                                     int
}

const (
	defaultAddress        = ":8083"
	defaultPollInterval   = 3 * time.Second
	defaultSourceTimeout  = 4 * time.Second
	defaultRetryBackoff   = 200 * time.Millisecond
	defaultQueryTimeout   = 2 * time.Second
	defaultPublishTimeout = 3 * time.Second
	defaultSourceRetries  = 2
	defaultPublishRetries = 2
	defaultQueryCapacity  = 64
	// A quarter of the query slots stay free for Field Team when Media fills the rest.
	defaultFieldReservedPercent = 25
	defaultPublishCapacity      = 256
	defaultIngestPool           = 2
	defaultQueryPool            = 8

	defaultHTTPReadHeaderTimeout = 3 * time.Second
	defaultHTTPReadTimeout       = 5 * time.Second
	defaultHTTPIdleTimeout       = 30 * time.Second
	defaultHTTPMaxHeaderBytes    = 16 << 10
	defaultStartupTimeout        = 15 * time.Second
	defaultShutdownTimeout       = 5 * time.Second
	defaultDBConnectTimeout      = 2 * time.Second
	defaultSourceIdleConnTimeout = 30 * time.Second
	defaultFailureMarkTimeout    = 2 * time.Second
	defaultWarningLookback       = time.Hour
	defaultBrokerHeartbeat       = 10 * time.Second
	defaultMaxSourceRecords      = 1000
	defaultMaxSourceBytes        = 4 << 20
	// One poll per second keeps the critical alert budget of 5 s at the 95th percentile.
	defaultSensorInterval = time.Second
	defaultSensorTimeout  = 2 * time.Second
	// A newest reading 5 minutes behind generated_at is expired data.
	defaultSensorStaleAfter = 5 * time.Minute
)

func ConfigFromEnv() (Config, error) {
	c := Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		RabbitMQURL:   os.Getenv("RABBITMQ_URL"),
		BMKGURL:       os.Getenv("BMKG_BASE_URL"),
		PVMBGURL:      os.Getenv("PVMBG_BASE_URL"),
		BMKGKey:       os.Getenv("BMKG_API_KEY"),
		PVMBGToken:    os.Getenv("PVMBG_TOKEN"),
		InternalToken: os.Getenv("AGGREGATOR_INTERNAL_TOKEN"),
		Address:       os.Getenv("HTTP_ADDR"),
		SensorURL:     os.Getenv("SENSOR_PANTAI_URL"),
		SensorToken:   os.Getenv("PANTAI_AUTH_TOKEN"),
	}
	for _, v := range []string{
		c.DatabaseURL,
		c.RabbitMQURL,
		c.BMKGURL,
		c.PVMBGURL,
		c.BMKGKey,
		c.PVMBGToken,
		c.InternalToken,
	} {
		if v == "" {
			return c, errors.New("required environment configuration missing")
		}
	}

	origins := []string{c.BMKGURL, c.PVMBGURL}
	if c.SensorURL != "" {
		origins = append(origins, c.SensorURL)
	}

	for _, raw := range origins {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil ||
			u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
			return c, errors.New("source URL must be an HTTP origin without credentials")
		}
	}

	if c.BMKGKey == c.PVMBGToken || c.InternalToken == c.BMKGKey || c.InternalToken == c.PVMBGToken {
		return c, errors.New("credentials must be independent")
	}

	if c.SensorURL != "" && c.SensorToken == "" {
		return c, errors.New("PANTAI_AUTH_TOKEN is required with SENSOR_PANTAI_URL")
	}

	if c.Address == "" {
		c.Address = defaultAddress
	}

	var (
		err                   error
		ingestPool, queryPool int
	)

	durations := []struct {
		key                string
		fallback, min, max time.Duration
		dest               *time.Duration
	}{
		{"POLL_INTERVAL", defaultPollInterval, time.Millisecond, time.Minute, &c.PollInterval},
		{"SOURCE_TIMEOUT", defaultSourceTimeout, time.Millisecond, time.Minute, &c.SourceTimeout},
		{"RETRY_BACKOFF", defaultRetryBackoff, time.Millisecond, time.Minute, &c.Backoff},
		{"QUERY_TIMEOUT", defaultQueryTimeout, time.Millisecond, time.Minute, &c.QueryTimeout},
		{"PUBLISH_TIMEOUT", defaultPublishTimeout, time.Millisecond, time.Minute, &c.PublishTimeout},
		{"HTTP_READ_HEADER_TIMEOUT", defaultHTTPReadHeaderTimeout, 100 * time.Millisecond, time.Minute, &c.HTTPReadHeaderTimeout},
		{"HTTP_READ_TIMEOUT", defaultHTTPReadTimeout, 100 * time.Millisecond, 5 * time.Minute, &c.HTTPReadTimeout},
		{"HTTP_IDLE_TIMEOUT", defaultHTTPIdleTimeout, time.Second, 10 * time.Minute, &c.HTTPIdleTimeout},
		{"STARTUP_TIMEOUT", defaultStartupTimeout, time.Second, 10 * time.Minute, &c.StartupTimeout},
		{"SHUTDOWN_TIMEOUT", defaultShutdownTimeout, time.Second, 2 * time.Minute, &c.ShutdownTimeout},
		{"DB_CONNECT_TIMEOUT", defaultDBConnectTimeout, 100 * time.Millisecond, time.Minute, &c.DBConnectTimeout},
		{"SOURCE_IDLE_CONN_TIMEOUT", defaultSourceIdleConnTimeout, time.Second, 10 * time.Minute, &c.SourceIdleConnTimeout},
		{"FAILURE_MARK_TIMEOUT", defaultFailureMarkTimeout, 100 * time.Millisecond, time.Minute, &c.FailureMarkTimeout},
		{"WARNING_LOOKBACK", defaultWarningLookback, time.Minute, 24 * time.Hour, &c.WarningLookback},
		{"BROKER_HEARTBEAT", defaultBrokerHeartbeat, time.Second, 5 * time.Minute, &c.BrokerHeartbeat},
		{"SENSOR_POLL_INTERVAL", defaultSensorInterval, 100 * time.Millisecond, time.Minute, &c.SensorInterval},
		{"SENSOR_TIMEOUT", defaultSensorTimeout, 100 * time.Millisecond, time.Minute, &c.SensorTimeout},
		{"SENSOR_STALE_AFTER", defaultSensorStaleAfter, time.Second, 24 * time.Hour, &c.SensorStaleAfter},
	}
	for _, d := range durations {
		if *d.dest, err = envDuration(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	ints := []struct {
		key                string
		fallback, min, max int
		dest               *int
	}{
		{"SOURCE_RETRIES", defaultSourceRetries, 0, 5, &c.SourceRetries},
		{"PUBLISH_RETRIES", defaultPublishRetries, 0, 5, &c.PublishRetries},
		{"QUERY_CAPACITY", defaultQueryCapacity, 1, 1024, &c.QueryCapacity},
		{"FIELD_TEAM_RESERVED_PERCENT", defaultFieldReservedPercent, 0, 90, &c.FieldReservedPercent},
		{"PUBLISH_CAPACITY", defaultPublishCapacity, 1, 10000, &c.PublishCapacity},
		{"INGEST_POOL_SIZE", defaultIngestPool, 1, 64, &ingestPool},
		{"QUERY_POOL_SIZE", defaultQueryPool, 1, 64, &queryPool},
		{"HTTP_MAX_HEADER_BYTES", defaultHTTPMaxHeaderBytes, 1 << 10, 1 << 20, &c.HTTPMaxHeaderBytes},
		{"MAX_SOURCE_RECORDS", defaultMaxSourceRecords, 1, 100000, &c.MaxSourceRecords},
		{"MAX_SOURCE_BYTES", defaultMaxSourceBytes, 64 << 10, defaultMaxSourceBytes, &c.MaxSourceBytes},
	}
	for _, d := range ints {
		if *d.dest, err = envInt(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	c.IngestPool, c.QueryPool = int32(ingestPool), int32(queryPool)

	return c, nil
}

func envDuration(key string, fallback, min, max time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d < min || d > max {
		return 0, fmt.Errorf("invalid %s", key)
	}

	return d, nil
}

func envInt(key string, fallback, min, max int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("invalid %s", key)
	}

	return n, nil
}
