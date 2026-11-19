package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/satellite"
)

type SatelliteSink struct {
	Data      ShardStore
	Epoch     int64
	Reference satellite.Reference
}

func (s *SatelliteSink) Load(ctx context.Context) ([]byte, error) {
	state, err := s.Data.LoadState(ctx, "SATELIT")
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	engine, err := satellite.Restore(state.Payload)
	if err != nil {
		return nil, err
	}

	if offset, hasOffset := engine.Position(); !hasOffset || offset != state.Offset {
		return nil, errors.New("satellite offset and state disagree")
	}

	return state.Payload, nil
}

// Guard is the single-instance development hook. The cluster leadership owner
// supplies a lease-renewing guard and the sealed epoch for production ingestion.
func (s *SatelliteSink) Guard(ctx context.Context) error {
	epoch, err := s.Data.Epoch(ctx, "SATELIT")
	if err != nil {
		return err
	}

	if epoch != s.Epoch {
		return satellite.ErrLeadershipLost
	}

	return nil
}

func (s *SatelliteSink) Commit(ctx context.Context, offset int64, payload []byte, fires []satellite.Fire) error {
	if len(s.Reference.Provinces) == 0 {
		return errors.New("satellite province reference required")
	}

	now := time.Now().UTC()

	events := make([]HazardEvent, 0, len(fires))
	for _, fire := range fires {
		attributes := map[string]json.RawMessage{}
		for key, value := range map[string]any{
			"detection_count": fire.Count, "satellites": fire.Satellites, "highest_confidence": fire.HighestConfidence,
			"total_frp_mw": fire.TotalFRP, "detection_keys": fire.Keys, "status": fire.Status,
			"province_reference": s.Reference.Version,
		} {
			attributes[key], _ = json.Marshal(value)
		}

		if fire.MergedInto != "" {
			attributes["merged_into"], _ = json.Marshal(fire.MergedInto)
		}

		events = append(events, HazardEvent{
			HazardID: CanonicalHazardID("SATELIT", fire.ID), Source: "SATELIT", SourceRefID: fire.ID, HazardType: "WILDFIRE", Severity: fire.Severity,
			AreaName: s.Reference.Nearest(fire.Latitude, fire.Longitude), Latitude: fire.Latitude, Longitude: fire.Longitude, OccurredAt: fire.OccurredAt, IngestedAt: now, Attributes: attributes, Revision: 1,
		})
	}

	_, err := s.Data.CommitState(ctx, WriteBatch{Source: "SATELIT", Epoch: s.Epoch, Events: events, Now: now}, StreamState{Offset: offset, Payload: payload})
	if errors.Is(err, ErrStaleEpoch) {
		return satellite.ErrLeadershipLost
	}

	return err
}
