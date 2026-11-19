package pvmbg

import (
	"encoding/json"
	"testing"
	"time"
)

func TestIncompatibleSchemaModesAffectOnlyNewReports(t *testing.T) {
	gen := NewGenerator(Seed, time.Unix(1, 0))

	old := gen.Next(time.Unix(2, 0), 1)
	for version := 1; version <= 5; version++ {
		r := gen.Next(time.Unix(int64(version+2), 0), version)

		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}

		var fields map[string]json.RawMessage
		if err = json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}

		_, canonicalAsh := fields["ash_column_height_m"]
		_, renamedAsh := fields["ash_height_m"]

		_, volcano := fields["volcano_id"]
		if canonicalAsh != (version != 3) || renamedAsh != (version == 3) || volcano != (version != 5) {
			t.Fatalf("version %d: %s", version, raw)
		}

		var count any
		if err = json.Unmarshal(fields["eruption_count_24h"], &count); err != nil {
			t.Fatal(err)
		}

		_, stringCount := count.(string)
		if stringCount != (version == 4) {
			t.Fatalf("version %d count type: %T", version, count)
		}
	}

	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}

	if fields["volcano_id"] == nil || fields["ash_column_height_m"] == nil || fields["confidence_level"] != nil {
		t.Fatalf("old report changed shape: %s", raw)
	}
}

func TestAdminAcceptsAllFiveSchemaModes(t *testing.T) {
	api := testAPI(t, 0)
	handler := api.Handler()

	for version := 1; version <= 5; version++ {
		raw, _ := json.Marshal(map[string]int{"version": version})

		response := request(handler, "POST", "/admin/schema-version", string(raw), nil)
		if response.Code != 200 || api.Controls.SchemaVersion() != version {
			t.Fatalf("version %d: status=%d body=%s", version, response.Code, response.Body.String())
		}
	}
}
