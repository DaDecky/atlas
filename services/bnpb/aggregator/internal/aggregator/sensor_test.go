package aggregator

import (
	"testing"

	"atlas/services/bnpb/aggregator/internal/sensor"
)

func TestCoastalSeverityFollowsTeamRule(t *testing.T) {
	for _, tc := range []struct {
		observation sensor.Observation
		want        string
	}{
		{sensor.Observation{SeaLevelAnomaly: 0.1, WaveHeight: 0.5}, "NORMAL"},
		{sensor.Observation{SeaLevelAnomaly: 0.5, WaveHeight: 0.5}, "WASPADA"},
		{sensor.Observation{SeaLevelAnomaly: 0.1, WaveHeight: 2.5}, "WASPADA"},
		{sensor.Observation{SeaLevelAnomaly: -1.0, WaveHeight: 0.5}, "SIAGA"},
		{sensor.Observation{SeaLevelAnomaly: 0.1, WaveHeight: 4.0}, "SIAGA"},
		{sensor.Observation{SeaLevelAnomaly: 0.1, WaveHeight: 0.5, Critical: true}, "AWAS"},
	} {
		if got := CoastalSeverity(tc.observation); got != tc.want {
			t.Errorf("%+v: got %s, want %s", tc.observation, got, tc.want)
		}
	}
}
