package pvmbg

import (
	"testing"
)

func TestSeedMeetsContract(t *testing.T) {
	reports, err := seedRecords(MinSeedRecords)
	if err != nil {
		t.Fatal(err)
	}

	if len(reports) < MinSeedRecords {
		t.Fatalf("seed has %d reports", len(reports))
	}

	levels, volcanoes := map[string]bool{}, map[string]bool{}

	for _, r := range reports {
		if err := validate(r); err != nil {
			t.Fatalf("seed report %s: %v", r.ReportID, err)
		}

		if r.ConfidenceLevel != nil {
			t.Fatalf("seed report %s has confidence_level before any schema change", r.ReportID)
		}

		levels[r.AlertLevel], volcanoes[r.VolcanoID] = true, true
	}

	if len(levels) != len(alertLevels) || len(volcanoes) != len(volcanoIDs) {
		t.Fatalf("seed covers alert levels %v and volcanoes %v", levels, volcanoes)
	}
}
