package auth

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type ClientCredential struct {
	ID     string
	Secret string
	Scope  string
}

type Config struct {
	DatabaseURL, ValidateToken, Address, Issuer, Audience string
	PrivateKey                                            ed25519.PrivateKey
	Clients                                               []ClientCredential
	AccessTTL, RefreshTTL                                 time.Duration

	HTTPReadHeaderTimeout, HTTPReadTimeout, HTTPWriteTimeout, HTTPIdleTimeout time.Duration
	StartupTimeout, ShutdownTimeout, DBConnectTimeout, StoreTimeout           time.Duration
	HTTPMaxHeaderBytes, DBPoolSize, MaxBodyBytes                              int
}

const (
	defaultAddress    = ":8084"
	defaultAccessTTL  = 60 * time.Second
	defaultRefreshTTL = 7 * 24 * time.Hour

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
	defaultMaxBodyBytes          = 8192
)

func decodeBase64(raw string) ([]byte, error) {
	if b, err := base64.RawStdEncoding.DecodeString(raw); err == nil {
		return b, nil
	}

	return base64.StdEncoding.DecodeString(raw)
}

func parsePrivateKey(raw string) (ed25519.PrivateKey, error) {
	encoded, err := decodeBase64(raw)
	if err != nil {
		return nil, errors.New("invalid signing key encoding")
	}

	block, _ := pem.Decode(encoded)
	if block == nil {
		return nil, errors.New("signing key must be a base64-encoded PKCS#8 PEM")
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid PKCS#8 signing key")
	}

	key, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("signing key must be Ed25519")
	}

	return key, nil
}

func durationEnv(name string, fallback, min, max time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d < min || d > max {
		return 0, fmt.Errorf("invalid %s", name)
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
		return 0, fmt.Errorf("invalid %s", name)
	}

	return n, nil
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		DatabaseURL:   os.Getenv("AUTH_DATABASE_URL"),
		ValidateToken: os.Getenv("AUTH_VALIDATE_TOKEN"),
		Address:       os.Getenv("AUTH_HTTP_ADDR"),
		Issuer:        "atlas-auth",
		Audience:      "atlas-client-api",
	}
	if c.DatabaseURL == "" || c.ValidateToken == "" {
		return c, errors.New("AUTH_DATABASE_URL and AUTH_VALIDATE_TOKEN are required")
	}

	key, err := parsePrivateKey(os.Getenv("AUTH_ED25519_PRIVATE_KEY_B64"))
	if err != nil {
		return c, err
	}

	c.PrivateKey = key
	if c.Address == "" {
		c.Address = defaultAddress
	}

	durations := []struct {
		key                string
		fallback, min, max time.Duration
		dest               *time.Duration
	}{
		{"ACCESS_TOKEN_TTL", defaultAccessTTL, time.Second, 365 * 24 * time.Hour, &c.AccessTTL},
		{"REFRESH_TOKEN_TTL", defaultRefreshTTL, time.Second, 365 * 24 * time.Hour, &c.RefreshTTL},
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
		{"DB_POOL_SIZE", defaultDBPoolSize, 1, 64, &c.DBPoolSize},
		{"AUTH_MAX_BODY_BYTES", defaultMaxBodyBytes, 512, 1 << 20, &c.MaxBodyBytes},
	}
	for _, d := range ints {
		if *d.dest, err = intEnv(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	c.Clients = []ClientCredential{
		{ID: os.Getenv("MEDIA_CLIENT_ID"), Secret: os.Getenv("MEDIA_CLIENT_SECRET"), Scope: "hazards:summary"},
		{ID: os.Getenv("FIELD_TEAM_CLIENT_ID"), Secret: os.Getenv("FIELD_TEAM_CLIENT_SECRET"), Scope: "hazards:raw"},
		{
			ID:     os.Getenv("INTERNAL_OPS_CLIENT_ID"),
			Secret: os.Getenv("INTERNAL_OPS_CLIENT_SECRET"),
			Scope:  "hazards:raw",
		},
	}

	seenID, seenSecret := map[string]bool{}, map[string]bool{}
	for _, client := range c.Clients {
		if client.ID == "" || client.Secret == "" || seenID[client.ID] || seenSecret[client.Secret] {
			return c, errors.New("three distinct client IDs and secrets are required")
		}

		seenID[client.ID], seenSecret[client.Secret] = true, true
	}

	return c, nil
}
