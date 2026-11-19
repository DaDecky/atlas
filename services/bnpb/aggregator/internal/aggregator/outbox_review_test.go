package aggregator

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCriticalAttributeMustBeBoolean(t *testing.T) {
	for _, raw := range []string{`"true"`, `1`, `null`, `{}`, `false`, `true`, ``} {
		t.Run(raw, func(t *testing.T) {
			h := contractEvent("SENSOR_PANTAI", "critical-type", time.Now().UTC())
			if raw != "" {
				h.Attributes["critical"] = json.RawMessage(raw)
			}

			critical, err := eventCritical(h)

			valid := raw == "" || raw == "false" || raw == "true"
			if valid && (err != nil || critical != (raw == "true")) {
				t.Fatal(critical, err)
			}

			if !valid && (err == nil || !strings.Contains(err.Error(), h.HazardID)) {
				t.Fatal("malformed critical silently accepted", err)
			}
		})
	}
}
