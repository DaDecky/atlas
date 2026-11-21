package pvmbg

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"strconv"
	"time"
)

type Config struct {
	Address            string
	DatabaseURL        string
	Token              string
	MinDelay, MaxDelay time.Duration
	EventInterval      time.Duration

	DelayFloor, DelayCeiling, EventIntervalCeiling                   time.Duration
	HTTPReadHeaderTimeout, HTTPReadTimeout, HTTPIdleTimeout, MaxHang time.Duration
	StartupTimeout, ShutdownTimeout, DBConnectTimeout, StoreTimeout  time.Duration
	HTTPMaxHeaderBytes, DBPoolSize, MinSeedRecords                   int
	Seed                                                             uint64
}

const (
	MinDelay         = 500 * time.Millisecond
	MaxDelay         = 3 * time.Second
	MaxEventInterval = 10 * time.Second

	defaultAddress       = ":8082"
	defaultMinDelay      = MinDelay
	defaultMaxDelay      = MaxDelay
	defaultEventInterval = MaxEventInterval

	defaultHTTPReadHeaderTimeout = 3 * time.Second
	defaultHTTPReadTimeout       = 5 * time.Second
	defaultHTTPIdleTimeout       = 30 * time.Second
	defaultHTTPMaxHeaderBytes    = 16 << 10
	defaultStartupTimeout        = 15 * time.Second
	defaultShutdownTimeout       = 5 * time.Second
	defaultDBConnectTimeout      = 2 * time.Second
	defaultDBPoolSize            = 8
	defaultStoreTimeout          = 2 * time.Second
)

// Matches pvmbg.<key-id>.<secret>, so BMKG's plain key never matches.
var tokenFormat = regexp.MustCompile(`^pvmbg\.[a-z0-9-]{1,32}\.[A-Za-z0-9_-]{16,128}$`)

func ConfigFromEnv() (Config, error) {
	c := Config{
		Address:     os.Getenv("HTTP_ADDR"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Token:       os.Getenv("PVMBG_TOKEN"),
	}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}

	if c.Address == "" {
		c.Address = defaultAddress
	}

	if !tokenFormat.MatchString(c.Token) {
		return c, errors.New("PVMBG_TOKEN must have the form pvmbg.<key-id>.<secret> with a secret of at least 16 URL-safe characters")
	}

	var err error
	if c.DelayFloor, err = envDuration("RESPONSE_DELAY_FLOOR", MinDelay, time.Millisecond, time.Minute); err != nil {
		return c, err
	}

	if c.DelayCeiling, err = envDuration("RESPONSE_DELAY_CEILING", MaxDelay, c.DelayFloor, time.Minute); err != nil {
		return c, err
	}

	if c.EventIntervalCeiling, err = envDuration("EVENT_INTERVAL_CEILING", MaxEventInterval, time.Second, time.Hour); err != nil {
		return c, err
	}

	if c.MinDelay, err = envDuration("MIN_RESPONSE_DELAY", min(max(defaultMinDelay, c.DelayFloor), c.DelayCeiling), c.DelayFloor, c.DelayCeiling); err != nil {
		return c, err
	}

	if c.MaxDelay, err = envDuration("MAX_RESPONSE_DELAY", min(max(defaultMaxDelay, c.DelayFloor), c.DelayCeiling), c.DelayFloor, c.DelayCeiling); err != nil {
		return c, err
	}

	if c.MinDelay > c.MaxDelay {
		return c, errors.New("MIN_RESPONSE_DELAY must not exceed MAX_RESPONSE_DELAY")
	}

	c.EventInterval, err = envDuration("EVENT_INTERVAL", c.EventIntervalCeiling, time.Second, c.EventIntervalCeiling)
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
		{"HTTP_IDLE_TIMEOUT", defaultHTTPIdleTimeout, time.Second, 10 * time.Minute, &c.HTTPIdleTimeout},
		{"OUTAGE_MAX_HANG", DefaultMaxHang, time.Second, time.Hour, &c.MaxHang},
		{"STARTUP_TIMEOUT", defaultStartupTimeout, time.Second, 10 * time.Minute, &c.StartupTimeout},
		{"SHUTDOWN_TIMEOUT", defaultShutdownTimeout, time.Second, 2 * time.Minute, &c.ShutdownTimeout},
		{"DB_CONNECT_TIMEOUT", defaultDBConnectTimeout, 100 * time.Millisecond, time.Minute, &c.DBConnectTimeout},
		{"STORE_TIMEOUT", defaultStoreTimeout, 100 * time.Millisecond, time.Minute, &c.StoreTimeout},
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
		{"HTTP_MAX_HEADER_BYTES", defaultHTTPMaxHeaderBytes, 1 << 10, 1 << 20, &c.HTTPMaxHeaderBytes},
		{"DB_POOL_SIZE", defaultDBPoolSize, 1, 64, &c.DBPoolSize},
		{"MIN_SEED_RECORDS", MinSeedRecords, 1, 100000, &c.MinSeedRecords},
	}
	for _, d := range ints {
		if *d.dest, err = envInt(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	if c.Seed, err = seedFromEnv(Seed); err != nil {
		return c, err
	}

	return c, nil
}

func envInt(key string, fallback, lowest, highest int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < lowest || n > highest {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, lowest, highest)
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

// Uniform in [MinDelay, MaxDelay], so equal bounds give a fixed delay.
func (c Config) RandomDelay() func() time.Duration {
	return func() time.Duration { return c.MinDelay + rand.N(c.MaxDelay-c.MinDelay+1) }
}

func envDuration(key string, fallback, lowest, highest time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d < lowest || d > highest {
		return 0, fmt.Errorf("%s must be a duration between %s and %s", key, lowest, highest)
	}

	return d, nil
}
