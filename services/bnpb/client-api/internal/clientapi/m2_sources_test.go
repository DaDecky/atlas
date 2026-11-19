package clientapi

import (
	"fmt"
	"net/http"
	"testing"
)

func TestPublicEnvelopeAcceptsM2Freshness(t *testing.T) {
	for _, source := range []string{"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"} {
		t.Run(source, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"data":[],"sources":[{"source":%q,"status":"healthy"}],"partial":false}`, source))
			if _, err := publicEnvelope(body, http.StatusOK, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestM2SourceAndHazardFilters(t *testing.T) {
	for _, query := range []string{"source=SATELIT", "source=SENSOR_PANTAI", "hazard_type=WILDFIRE", "hazard_type=COASTAL"} {
		if _, _, err := allowedFilters(query); err != nil {
			t.Fatal(query, err)
		}
	}
}
