package bmkg

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Address       string
	DatabaseURL   string
	APIKey        string
	Delay         time.Duration
	EventInterval time.Duration

	DelayFloor, DelayCeiling, EventIntervalCeiling                            time.Duration
	HTTPReadHeaderTimeout, HTTPReadTimeout, HTTPWriteTimeout, HTTPIdleTimeout time.Duration
	StartupTimeout, ShutdownTimeout, DBConnectTimeout, StoreTimeout           time.Duration
	HTTPMaxHeaderBytes, DBPoolSize, MinSeedRecords                            int
	Seed                                                                      uint64
}

const (
	MinDelay         = 50 * time.Millisecond
	MaxDelay         = 150 * time.Millisecond
	MaxEventInterval = 10 * time.Second

	defaultAddress       = ":8081"
	defaultDelay         = 100 * time.Millisecond
	defaultEventInterval = MaxEventInterval

	defaultHTTPReadHeaderTimeout = 3 * time.Second
	defaultHTTPReadTimeout       = 5 * time.Second
	defaultHTTPWriteTimeout      = 5 * time.Second
	defaultHTTPIdleTimeout       = 30 * time.Second
	defaultHTTPMaxHeaderBytes    = 16 << 10
	defaultStartupTimeout        = 15 * time.Second
	defaultShutdownTimeout       = 5 * time.Second
	defaultDBConnectTimeout      = 2 * time.Second
	defaultDBPoolSize            = 8
	defaultStoreTimeout          = 2 * time.Second
)

func ConfigFromEnv() (Config, error) {
	c := Config{
		Address:     os.Getenv("HTTP_ADDR"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		APIKey:      os.Getenv("BMKG_API_KEY"),
	}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}

	if c.Address == "" {
		c.Address = defaultAddress
	}

	if c.APIKey == "" || strings.ContainsAny(c.APIKey, " \t\r\n") {
		return c, errors.New("BMKG_API_KEY is required and must not contain whitespace")
	}

	var err error
	if c.DelayFloor, err = duration("RESPONSE_DELAY_FLOOR", MinDelay, time.Millisecond, time.Minute); err != nil {
		return c, err
	}

	if c.DelayCeiling, err = duration("RESPONSE_DELAY_CEILING", MaxDelay, c.DelayFloor, time.Minute); err != nil {
		return c, err
	}

	if c.EventIntervalCeiling, err = duration("EVENT_INTERVAL_CEILING", MaxEventInterval, time.Second, time.Hour); err != nil {
		return c, err
	}

	if c.Delay, err = duration("RESPONSE_DELAY", min(max(defaultDelay, c.DelayFloor), c.DelayCeiling), c.DelayFloor, c.DelayCeiling); err != nil {
		return c, err
	}

	c.EventInterval, err = duration("EVENT_INTERVAL", c.EventIntervalCeiling, time.Second, c.EventIntervalCeiling)
	if err != nil {
		return c, err
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
		{"STARTUP_TIMEOUT", defaultStartupTimeout, time.Second, 10 * time.Minute, &c.StartupTimeout},
		{"SHUTDOWN_TIMEOUT", defaultShutdownTimeout, time.Second, 2 * time.Minute, &c.ShutdownTimeout},
		{"DB_CONNECT_TIMEOUT", defaultDBConnectTimeout, 100 * time.Millisecond, time.Minute, &c.DBConnectTimeout},
		{"STORE_TIMEOUT", defaultStoreTimeout, 100 * time.Millisecond, time.Minute, &c.StoreTimeout},
	}
	for _, d := range durations {
		if *d.dest, err = duration(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	ints := []struct {
		key                string
		fallback, min, max int
		dest               *int
	}{
		{"HTTP_MAX_HEADER_BYTES", defaultHTTPMaxHeaderBytes, 1 << 10, 1 << 20, &c.HTTPMaxHeaderBytes},
		{"DB_POOL_SIZE", defaultDBPoolSize, 1, 64, &c.DBPoolSize},
		{"MIN_SEED_RECORDS", MinSeedRecords, 1, 100000, &c.MinSeedRecords},
	}
	for _, d := range ints {
		if *d.dest, err = integer(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	if c.Seed, err = seedFromEnv(Seed); err != nil {
		return c, err
	}

	return c, nil
}

func integer(key string, fallback, min, max int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, min, max)
	}

	return n, nil
}

func seedFromEnv(fallback uint64) (uint64, error) {
	raw := os.Getenv("SEED")
	if raw == "" {
		return fallback, nil
	}

	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, errors.New("SEED must be an unsigned integer")
	}

	return n, nil
}

func duration(key string, fallback, min, max time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d < min || d > max {
		return 0, fmt.Errorf("%s must be a duration between %s and %s", key, min, max)
	}

	return d, nil
}
