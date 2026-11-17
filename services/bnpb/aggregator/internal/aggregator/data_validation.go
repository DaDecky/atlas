package aggregator

import (
	"encoding/json"
	"errors"
	"math"
)

func validateBatch(b WriteBatch) error {
	if !validSource(b.Source) || b.Now.IsZero() {
		return errors.New("valid source and ingestion time required")
	}

	if b.Epoch < 1 {
		return ErrStaleEpoch
	}

	for _, h := range b.Events {
		if h.Source != b.Source || h.HazardID == "" || h.SourceRefID == "" || h.OccurredAt.IsZero() || h.IngestedAt.IsZero() || h.Attributes == nil {
			return errors.New("invalid event identity or timestamps")
		}

		expected := map[string]string{"BMKG": "SEISMIC", "PVMBG": "VOLCANIC", "SATELIT": "WILDFIRE", "SENSOR_PANTAI": "COASTAL"}
		if h.HazardType != expected[h.Source] {
			return errors.New("hazard type differs from source")
		}

		if h.Severity != "NORMAL" && h.Severity != "WASPADA" && h.Severity != "SIAGA" && h.Severity != "AWAS" {
			return errors.New("invalid severity")
		}

		if h.Latitude < -90 || h.Latitude > 90 || h.Longitude < -180 || h.Longitude > 180 {
			return errors.New("coordinates out of range")
		}
		if math.IsNaN(h.Latitude) || math.IsNaN(h.Longitude) || math.IsInf(h.Latitude, 0) || math.IsInf(h.Longitude, 0) {
			return errors.New("non-finite coordinates")
		}

		if _, err := json.Marshal(h.Attributes); err != nil {
			return err
		}
	}

	return nil
}

func validSource(source string) bool {
	return source == "BMKG" || source == "PVMBG" || source == "SATELIT" || source == "SENSOR_PANTAI"
}
