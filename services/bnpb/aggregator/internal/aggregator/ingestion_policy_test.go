package aggregator

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestSchemaPolicyReorderedKeysWhitespaceAndIgnoredField(t *testing.T) {
	base := fixture(t, "volcanic-v1.json")[0]
	now := time.Now()

	want, err := (SchemaPolicy{}).MapVolcanic(base, now)
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]string, 0, len(base))
	for key := range base {
		keys = append(keys, key)
	}

	sort.Sort(sort.Reverse(sort.StringSlice(keys)))

	var wire bytes.Buffer
	wire.WriteString("{ \n")

	for _, key := range keys {
		rawKey, _ := json.Marshal(key)
		wire.Write(rawKey)
		wire.WriteString(" : \t")
		wire.Write(base[key])
		wire.WriteString(", \n")
	}

	// confidence_level is optional and retained as an extension, but does not
	// change the required canonical classification or event identity.
	wire.WriteString(`"confidence_level" : 0.8 }`)

	var reordered Object
	if err := json.Unmarshal(wire.Bytes(), &reordered); err != nil {
		t.Fatal(err)
	}

	got, err := (SchemaPolicy{}).MapVolcanic(reordered, now)
	if err != nil {
		t.Fatal(err)
	}

	if string(got.Attributes["confidence_level"]) != "0.8" {
		t.Fatal("optional extension lost")
	}

	delete(got.Attributes, "confidence_level")
	delete(want.Attributes, "confidence_level")

	if !reflect.DeepEqual(got, want) {
		t.Fatal("wire ordering or optional field changed canonical mapping")
	}
}

func TestSchemaPolicyTable(t *testing.T) {
	base := fixture(t, "volcanic-v1.json")[0]

	cases := []struct {
		name   string
		change func(Object)
		policy SchemaPolicy
		accept bool
	}{
		{"original", func(Object) {}, SchemaPolicy{}, true},
		{"additive", func(o Object) { o["new_measurement"] = json.RawMessage(`{"value":4}`) }, SchemaPolicy{}, true},
		{"optional-omitted", func(o Object) { delete(o, "confidence_level") }, SchemaPolicy{}, true},
		{"optional-present", func(o Object) { o["confidence_level"] = json.RawMessage(`0.8`) }, SchemaPolicy{}, true},
		{"rename-default", func(o Object) { o["ash_height_m"] = o["ash_column_height_m"]; delete(o, "ash_column_height_m") }, SchemaPolicy{}, false},
		{"rename-explicit", func(o Object) { o["ash_height_m"] = o["ash_column_height_m"]; delete(o, "ash_column_height_m") }, SchemaPolicy{AcceptAshRename: true}, true},
		{"type-default", func(o Object) { o["eruption_count_24h"] = json.RawMessage(`"3"`) }, SchemaPolicy{}, false},
		{"type-explicit", func(o Object) { o["eruption_count_24h"] = json.RawMessage(`"3"`) }, SchemaPolicy{AcceptStringCount: true}, true},
		{"invalid-conversion", func(o Object) { o["eruption_count_24h"] = json.RawMessage(`"3.5"`) }, SchemaPolicy{AcceptStringCount: true}, false},
		{"missing-default", func(o Object) { delete(o, "volcano_id") }, SchemaPolicy{}, false},
		{"missing-reference", func(o Object) { delete(o, "volcano_id") }, SchemaPolicy{VolcanoByReport: map[string]string{"report-1": "merapi"}}, true},
		{"missing-no-guess", func(o Object) { delete(o, "volcano_id") }, SchemaPolicy{VolcanoByReport: map[string]string{"another-report": "merapi"}}, false},
		{"unknown-enum", func(o Object) { o["alert_level"] = json.RawMessage(`"Extreme"`) }, SchemaPolicy{}, false},
		{"unknown-reference", func(o Object) { o["volcano_id"] = json.RawMessage(`"unknown"`) }, SchemaPolicy{}, false},
		{"null-required", func(o Object) { o["volcano_id"] = json.RawMessage(`null`) }, SchemaPolicy{}, false},
		{"invalid-optional", func(o Object) { o["confidence_level"] = json.RawMessage(`2`) }, SchemaPolicy{}, false},
		{"rename-conflict", func(o Object) { o["ash_height_m"] = json.RawMessage(`99`) }, SchemaPolicy{AcceptAshRename: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := clone(base)
			tc.change(o)
			before, _ := json.Marshal(o)

			h, err := tc.policy.MapVolcanic(o, time.Now())
			if (err == nil) != tc.accept {
				t.Fatalf("accepted=%v want=%v: %v", err == nil, tc.accept, err)
			}

			after, _ := json.Marshal(o)
			if !bytes.Equal(before, after) {
				t.Fatal("policy mutated source payload")
			}

			if tc.name == "additive" && h.Attributes["new_measurement"] == nil {
				t.Fatal("additive attribute was discarded")
			}
		})
	}
}

func TestSchemaPolicyFileRejectsInvalidConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	for _, raw := range []string{`{"accept_string_count":true,"typo":true}`, `{"volcano_by_report":{"r":"unknown"}}`, `{} {}`, `null`} {
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}

		if _, err := ReadSchemaPolicy(path); err == nil {
			t.Fatalf("accepted invalid policy: %s", raw)
		}
	}
}
