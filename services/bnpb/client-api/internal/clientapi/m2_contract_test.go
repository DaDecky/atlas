package clientapi

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestM2ScopeSetsAndRawAccess(t *testing.T) {
	for _, scopes := range []string{"hazards:summary", "hazards:raw", "hazards:raw reports:write", "hazards:raw ops:read"} {
		if !validScopes(scopes) {
			t.Fatal("valid scope set rejected", scopes)
		}
	}

	for _, scopes := range []string{"", "hazards:raw admin", "reports:write", "hazards:raw hazards:raw"} {
		if validScopes(scopes) {
			t.Fatal("invalid scope set accepted", scopes)
		}
	}

	if hasScope("hazards:raw reports:write", "ops:read") || !hasScope("hazards:raw ops:read", "ops:read") {
		t.Fatal("scope membership confused identities")
	}
}

func TestM2FiltersAndPublicMetadata(t *testing.T) {
	for _, query := range []string{"source=SATELIT&hazard_type=WILDFIRE&cursor=abc", "source=SENSOR_PANTAI&hazard_type=COASTAL"} {
		if _, _, err := allowedFilters(query); err != nil {
			t.Fatal(query, err)
		}
	}

	if _, _, err := allowedFilters("source=SATELIT&hazard_type=SEISMIC"); err == nil {
		t.Fatal("contradictory M2 filters accepted")
	}

	body := []byte(`{"data":[],"sources":[{"source":"SATELIT","status":"healthy"},{"source":"SENSOR_PANTAI","status":"unavailable"}],"partial":true,"unavailable_units":["shard-3"],"next_cursor":"resume","debug":"hidden"}`)

	out, err := publicEnvelope(body, 200, true)
	if err != nil {
		t.Fatal(err)
	}

	var envelope struct {
		Partial bool     `json:"partial"`
		Cursor  string   `json:"next_cursor"`
		Units   []string `json:"unavailable_units"`
	}
	if json.Unmarshal(out, &envelope) != nil || !envelope.Partial || envelope.Cursor != "resume" || len(envelope.Units) != 1 || strings.Contains(string(out), "hidden") {
		t.Fatal("M2 pagination/partial metadata lost or diagnostics leaked", string(out))
	}
}

func TestFieldReportInputBoundary(t *testing.T) {
	base := `{"report_id":"r-1","observed_at":"2026-11-18T00:00:00Z","location":{"latitude":-6.2,"longitude":106.8,"area_name":"Jakarta"},"category":"COASTAL","message":"Rising water"}`
	if _, err := decodeReport([]byte(base)); err != nil {
		t.Fatal(err)
	}

	for _, raw := range []string{
		strings.Replace(base, `"r-1"`, `""`, 1),
		strings.Replace(base, `"latitude":-6.2`, `"latitude":null`, 1),
		strings.Replace(base, `"longitude":106.8`, `"longitude":181`, 1),
		strings.Replace(base, `"COASTAL"`, `"unknown"`, 1),
		strings.TrimSuffix(base, "}") + `,"client_id":"spoofed"}`,
		base + " {}",
	} {
		if _, err := decodeReport([]byte(raw)); err == nil {
			t.Fatal("invalid report accepted", raw)
		}
	}
}
