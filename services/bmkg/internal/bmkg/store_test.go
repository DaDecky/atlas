package bmkg

import (
	"testing"
)

func TestSeedMeetsContract(t *testing.T) {
	events, warnings, err := seedRecords(MinSeedRecords)
	if err != nil {
		t.Fatal(err)
	}

	if len(events) < MinSeedRecords {
		t.Fatalf("seed has %d events", len(events))
	}

	magnitudes := map[float64]bool{}
	levels := map[string]bool{}

	var several, unwarned, calm bool

	for _, e := range events {
		magnitudes[e.Magnitude] = true
		if err := validate(e, warnings[e.EventID]); err != nil {
			t.Fatalf("seed event %s: %v", e.EventID, err)
		}

		for _, w := range warnings[e.EventID] {
			levels[w.ThreatLevel] = true
		}

		several = several || len(warnings[e.EventID]) > 1
		unwarned = unwarned || e.PotentialTsunami && len(warnings[e.EventID]) == 0
		calm = calm || !e.PotentialTsunami
	}

	for _, m := range []float64{4.9, 5.0, 6.49, 6.5} {
		if !magnitudes[m] {
			t.Fatalf("seed misses magnitude boundary %v", m)
		}
	}

	if !several || !unwarned || !calm || len(levels) != len(threatLevels) {
		t.Fatalf(
			"seed needs several warnings on one event (%v), a potential tsunami without warning (%v), calm events (%v), and every threat level %v",
			several, unwarned, calm, levels,
		)
	}
}
