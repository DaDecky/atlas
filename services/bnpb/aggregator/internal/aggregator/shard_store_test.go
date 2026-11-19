package aggregator

import (
	"encoding/json"
	"time"
)

func contractEvent(source, ref string, now time.Time) HazardEvent {
	types := map[string]string{"BMKG": "SEISMIC", "PVMBG": "VOLCANIC", "SATELIT": "WILDFIRE", "SENSOR_PANTAI": "COASTAL"}

	return HazardEvent{
		HazardID: hazardID(source, ref), Source: source, SourceRefID: ref,
		HazardType: types[source], Severity: "WASPADA", AreaName: "test",
		OccurredAt: now, IngestedAt: now, Attributes: map[string]json.RawMessage{},
	}
}
