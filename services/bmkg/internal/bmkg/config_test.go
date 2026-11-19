package bmkg

import (
	"testing"
	"time"
)

func validTestEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BMKG_API_KEY", "test-bmkg")
	t.Setenv("DATABASE_URL", "postgres://bmkg:test@localhost/bmkg")

	for _, key := range []string{
		"HTTP_ADDR", "RESPONSE_DELAY", "EVENT_INTERVAL", "RESPONSE_DELAY_FLOOR", "RESPONSE_DELAY_CEILING",
		"EVENT_INTERVAL_CEILING", "MIN_SEED_RECORDS", "SEED", "DB_POOL_SIZE", "STORE_TIMEOUT", "HTTP_READ_TIMEOUT",
	} {
		t.Setenv(key, "")
	}
}

func TestConfigLimitsOverrideAndBounds(t *testing.T) {
	validTestEnv(t)

	t.Setenv("MIN_SEED_RECORDS", "5")
	t.Setenv("RESPONSE_DELAY_CEILING", "500ms")
	t.Setenv("RESPONSE_DELAY", "300ms")
	t.Setenv("SEED", "7")

	c, err := ConfigFromEnv()
	if err != nil || c.MinSeedRecords != 5 || c.Delay != 300*time.Millisecond || c.Seed != 7 {
		t.Fatal(c, err)
	}

	for key, value := range map[string]string{"MIN_SEED_RECORDS": "0", "DB_POOL_SIZE": "100", "SEED": "x"} {
		t.Setenv(key, value)

		if _, err = ConfigFromEnv(); err == nil {
			t.Fatal(key, "accepted")
		}

		t.Setenv(key, "")
	}
}

func TestConfigDefaultsAndContractBounds(t *testing.T) {
	validTestEnv(t)

	c, err := ConfigFromEnv()
	if err != nil || c.Address != ":8081" || c.APIKey != "test-bmkg" ||
		c.Delay != 100*time.Millisecond || c.EventInterval != 10*time.Second {
		t.Fatal(c, err)
	}

	for _, tc := range []struct{ key, value string }{
		{"RESPONSE_DELAY", "50ms"},
		{"RESPONSE_DELAY", "150ms"},
		{"EVENT_INTERVAL", "1s"},
		{"EVENT_INTERVAL", "10s"},
	} {
		t.Run("accept "+tc.key+"="+tc.value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv(tc.key, tc.value)

			if _, err := ConfigFromEnv(); err != nil {
				t.Fatal(err)
			}
		})
	}

	for _, tc := range []struct{ key, value string }{
		{"DATABASE_URL", ""},
		{"BMKG_API_KEY", ""},
		{"BMKG_API_KEY", "key with space"},
		{"RESPONSE_DELAY", "49ms"},
		{"RESPONSE_DELAY", "151ms"},
		{"RESPONSE_DELAY", "fast"},
		{"EVENT_INTERVAL", "0s"},
		{"EVENT_INTERVAL", "-1s"},
		{"EVENT_INTERVAL", "11s"},
	} {
		t.Run("reject "+tc.key+"="+tc.value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv(tc.key, tc.value)

			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
