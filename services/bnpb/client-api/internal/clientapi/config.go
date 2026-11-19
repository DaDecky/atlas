package clientapi

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	AuthURL, AggregatorURL, AggregatorToken, ValidateToken, Address string
	Issuer, Audience                                                string
	PublicKey                                                       ed25519.PublicKey
	OutboundTimeout                                                 time.Duration
	RequestCapacity                                                 int

	HTTPReadHeaderTimeout, HTTPReadTimeout, HTTPIdleTimeout, ShutdownTimeout, MaxRetryAfter time.Duration
	HTTPMaxHeaderBytes, MaxResponseBytes, MaxValidateBytes                                  int
}

const (
	defaultAddress         = ":8080"
	defaultOutboundTimeout = 3 * time.Second
	defaultRequestCapacity = 128

	defaultHTTPReadHeaderTimeout = 3 * time.Second
	defaultHTTPReadTimeout       = 5 * time.Second
	defaultHTTPIdleTimeout       = 30 * time.Second
	defaultHTTPMaxHeaderBytes    = 16 << 10
	defaultShutdownTimeout       = 5 * time.Second
	defaultMaxRetryAfter         = time.Hour
	defaultMaxResponseBytes      = 4 << 20
	defaultMaxValidateBytes      = 4096
)

func decodeBase64(raw string) ([]byte, error) {
	if b, err := base64.RawStdEncoding.DecodeString(raw); err == nil {
		return b, nil
	}

	return base64.StdEncoding.DecodeString(raw)
}

func parsePublicKey(raw string) (ed25519.PublicKey, error) {
	encoded, err := decodeBase64(raw)
	if err != nil {
		return nil, errors.New("invalid public key encoding")
	}

	block, _ := pem.Decode(encoded)
	if block == nil {
		return nil, errors.New("public key must be a base64-encoded SubjectPublicKeyInfo PEM")
	}

	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid SubjectPublicKeyInfo key")
	}

	key, ok := parsed.(ed25519.PublicKey)
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("verification key must be Ed25519")
	}

	return key, nil
}

func requireOrigin(name, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" ||
		u.Fragment != "" || u.Path != "" {
		return fmt.Errorf("%s must be an HTTP origin without credentials", name)
	}

	return nil
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		AuthURL: os.Getenv("AUTH_BASE_URL"), AggregatorURL: os.Getenv("AGGREGATOR_BASE_URL"),
		AggregatorToken: os.Getenv("AGGREGATOR_INTERNAL_TOKEN"), ValidateToken: os.Getenv("AUTH_VALIDATE_TOKEN"),
		Address: os.Getenv("CLIENT_API_HTTP_ADDR"), Issuer: "atlas-auth", Audience: "atlas-client-api",
	}
	for name, value := range map[string]string{
		"AUTH_BASE_URL":             c.AuthURL,
		"AGGREGATOR_BASE_URL":       c.AggregatorURL,
		"AGGREGATOR_INTERNAL_TOKEN": c.AggregatorToken,
		"AUTH_VALIDATE_TOKEN":       c.ValidateToken,
	} {
		if value == "" {
			return c, fmt.Errorf("%s is required", name)
		}
	}

	if err := requireOrigin("AUTH_BASE_URL", c.AuthURL); err != nil {
		return c, err
	}

	if err := requireOrigin("AGGREGATOR_BASE_URL", c.AggregatorURL); err != nil {
		return c, err
	}

	key, err := parsePublicKey(os.Getenv("AUTH_ED25519_PUBLIC_KEY_B64"))
	if err != nil {
		return c, err
	}

	c.PublicKey = key
	if c.Address == "" {
		c.Address = defaultAddress
	}

	c.OutboundTimeout = defaultOutboundTimeout
	if raw := os.Getenv("CLIENT_API_OUTBOUND_TIMEOUT"); raw != "" {
		c.OutboundTimeout, err = time.ParseDuration(raw)
		if err != nil || c.OutboundTimeout < time.Second || c.OutboundTimeout > time.Minute {
			return c, errors.New("invalid CLIENT_API_OUTBOUND_TIMEOUT")
		}
	}

	c.RequestCapacity = defaultRequestCapacity
	if raw := os.Getenv("CLIENT_API_REQUEST_CAPACITY"); raw != "" {
		c.RequestCapacity, err = strconv.Atoi(raw)
		if err != nil || c.RequestCapacity < 1 || c.RequestCapacity > 10000 {
			return c, errors.New("invalid CLIENT_API_REQUEST_CAPACITY")
		}
	}

	durations := []struct {
		key                string
		fallback, min, max time.Duration
		dest               *time.Duration
	}{
		{"HTTP_READ_HEADER_TIMEOUT", defaultHTTPReadHeaderTimeout, 100 * time.Millisecond, time.Minute, &c.HTTPReadHeaderTimeout},
		{"HTTP_READ_TIMEOUT", defaultHTTPReadTimeout, 100 * time.Millisecond, 5 * time.Minute, &c.HTTPReadTimeout},
		{"HTTP_IDLE_TIMEOUT", defaultHTTPIdleTimeout, time.Second, 10 * time.Minute, &c.HTTPIdleTimeout},
		{"SHUTDOWN_TIMEOUT", defaultShutdownTimeout, time.Second, 2 * time.Minute, &c.ShutdownTimeout},
		{"MAX_RETRY_AFTER", defaultMaxRetryAfter, time.Second, 24 * time.Hour, &c.MaxRetryAfter},
	}
	for _, d := range durations {
		if *d.dest, err = durationEnv(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	ints := []struct {
		key                string
		fallback, min, max int
		dest               *int
	}{
		{"HTTP_MAX_HEADER_BYTES", defaultHTTPMaxHeaderBytes, 1 << 10, 1 << 20, &c.HTTPMaxHeaderBytes},
		{"AGGREGATOR_MAX_RESPONSE_BYTES", defaultMaxResponseBytes, 64 << 10, 64 << 20, &c.MaxResponseBytes},
		{"AUTH_VALIDATE_MAX_BYTES", defaultMaxValidateBytes, 512, 1 << 20, &c.MaxValidateBytes},
	}
	for _, d := range ints {
		if *d.dest, err = intEnv(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	return c, nil
}

func durationEnv(name string, fallback, min, max time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d < min || d > max {
		return 0, errors.New("invalid " + name)
	}

	return d, nil
}

func intEnv(name string, fallback, min, max int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, errors.New("invalid " + name)
	}

	return n, nil
}
