package aggregator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type HazardEvent struct {
	HazardID    string                     `json:"hazard_id"`
	Source      string                     `json:"source"`
	SourceRefID string                     `json:"source_ref_id"`
	HazardType  string                     `json:"hazard_type"`
	Severity    string                     `json:"severity"`
	AreaName    string                     `json:"area_name"`
	Latitude    float64                    `json:"latitude"`
	Longitude   float64                    `json:"longitude"`
	OccurredAt  time.Time                  `json:"occurred_at"`
	IngestedAt  time.Time                  `json:"ingested_at"`
	Attributes  map[string]json.RawMessage `json:"attributes"`
	Revision    int64                      `json:"revision"`
}

type Freshness struct {
	Source        string     `json:"source"`
	Status        string     `json:"status"`
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`
}

type Query struct {
	Source, HazardType, Severity string
	Since                        *time.Time
	Limit                        int
	Cursor                       string
	Audit                        bool
}

type Response struct {
	Data             []HazardEvent `json:"data"`
	Sources          []Freshness   `json:"sources"`
	Partial          bool          `json:"partial"`
	UnavailableUnits []string      `json:"unavailable_units"`
	NextCursor       *string       `json:"next_cursor"`
}

// The four sources, their hazard types, and the severities are named here once.
const (
	SourceBMKG         = "BMKG"
	SourcePVMBG        = "PVMBG"
	SourceSatellite    = "SATELIT"
	SourceSensorPantai = "SENSOR_PANTAI"
)

const (
	SeverityNormal  = "NORMAL"
	SeverityWaspada = "WASPADA"
	SeveritySiaga   = "SIAGA"
	SeverityAwas    = "AWAS"
)

// Sources lists every source in the order that reads report them.
var Sources = []string{SourceBMKG, SourcePVMBG, SourceSatellite, SourceSensorPantai}

var sourceTypes = map[string]string{SourceBMKG: "SEISMIC", SourcePVMBG: "VOLCANIC", SourceSatellite: "WILDFIRE", SourceSensorPantai: "COASTAL"}

var severities = map[string]bool{SeverityNormal: true, SeverityWaspada: true, SeveritySiaga: true, SeverityAwas: true}

func hazardID(source, ref string) string {
	sum := sha256.Sum256([]byte(source + "\x00" + ref))
	return hex.EncodeToString(sum[:])
}

func sources(q Query) []string {
	if q.Source != "" {
		return []string{q.Source}
	}

	for source, hazardType := range sourceTypes {
		if q.HazardType == hazardType {
			return []string{source}
		}
	}

	return Sources
}

// CanonicalHazardID is shared by source adapters and data tools. A replay must
// retain the same source/reference pair to address the same canonical event.
func CanonicalHazardID(source, ref string) string { return hazardID(source, ref) }

// deriveFreshness gives a source with no usable data unavailable, a source that never
// succeeded or whose last poll failed stale, and every other source healthy.
func deriveFreshness(lastSuccess, staleSince *time.Time, hasEvents bool) string {
	if !hasEvents {
		return "unavailable"
	}

	if lastSuccess == nil || staleSince != nil {
		return "stale"
	}

	return "healthy"
}
