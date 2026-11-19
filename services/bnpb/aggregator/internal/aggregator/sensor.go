package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/sensor"
)

// SensorPoller reads Sensor Pantai pages under one writer epoch. The cursor and the last
// position commit with the events, so a restart resumes where the store stands.
type SensorPoller struct {
	Client               *sensor.Client
	Ingest               *EpochIngest
	Interval, StaleAfter time.Duration
	Guard                func(context.Context) error
	Log                  *slog.Logger
	stations             map[string]sensor.Station
}

type sensorState struct {
	Cursor string          `json:"cursor"`
	Last   sensor.Position `json:"last"`
}

// SensorSource is the source name of the Sensor Pantai.
const SensorSource = "SENSOR_PANTAI"

const sensorPageLimit = 100

// Lampiran C gives no severity rule, so this team rule ranks the size of the sea level
// anomaly, either sign, and the wave height. The sensor's own critical flag means AWAS.
var coastalLevels = []struct {
	severity      string
	anomaly, wave float64
}{
	{"SIAGA", 1.0, 4.0},
	{"WASPADA", 0.5, 2.5},
}

func CoastalSeverity(o sensor.Observation) string {
	if o.Critical {
		return "AWAS"
	}

	for _, level := range coastalLevels {
		if math.Abs(o.SeaLevelAnomaly) >= level.anomaly || o.WaveHeight >= level.wave {
			return level.severity
		}
	}

	return "NORMAL"
}

// Run polls at a fixed interval, and reads the next page at once while a backlog remains.
func (p *SensorPoller) Run(ctx context.Context) {
	for ctx.Err() == nil {
		id := correlationID()

		more, err := p.Poll(ctx, id)
		if errors.Is(err, ErrStaleEpoch) {
			return
		}

		if err != nil && ctx.Err() == nil {
			p.fail(ctx, id, err)
		}

		if more {
			continue
		}

		if !pause(ctx, p.Interval) {
			return
		}
	}
}

func (p *SensorPoller) Poll(ctx context.Context, id string) (bool, error) {
	state, offset, err := p.load(ctx)
	if err != nil {
		return false, fmt.Errorf("load sensor state: %w", err)
	}

	page, err := p.Client.Observations(ctx, state.Cursor, sensorPageLimit)
	if err != nil {
		return false, err
	}

	received := time.Now().UTC()
	more := len(page.Items) == sensorPageLimit && page.NextCursor != state.Cursor
	items, last, health := sensor.Assess(page, state.Last, p.StaleAfter)

	// Old readings in a backlog page are old because of the catch-up, not because the sensor is sick.
	health.Stale = health.Stale && !more

	parked, err := p.parkViolations(ctx, id, page, received)
	if err != nil {
		return false, err
	}

	events, unknown, err := p.events(ctx, id, items, received)
	if err != nil {
		return false, err
	}

	health.Violations += unknown

	if err = p.commit(ctx, events, offset, sensorState{Cursor: page.NextCursor, Last: last}, received); err != nil {
		return false, err
	}

	p.report(ctx, id, health, len(events), parked+unknown, received)

	return more, nil
}

func (p *SensorPoller) load(ctx context.Context) (sensorState, int64, error) {
	var state sensorState

	stored, err := p.Ingest.Data.LoadState(ctx, SensorSource)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, 0, nil
	}

	if err != nil {
		return state, 0, err
	}

	if err = json.Unmarshal(stored.Payload, &state); err != nil {
		return state, 0, fmt.Errorf("decode sensor state: %w", err)
	}

	return state, stored.Offset, nil
}

func (p *SensorPoller) parkViolations(ctx context.Context, id string, page sensor.Page, received time.Time) (int, error) {
	parked := 0

	for _, item := range page.Items {
		if item.Violation == "" {
			continue
		}

		if err := p.park(ctx, id, item.Raw, item.Observation.ID, item.Violation, received); err != nil {
			return parked, err
		}

		parked++
	}

	return parked, nil
}

// events maps observations to COASTAL events. An unknown station has no location, so it is parked.
func (p *SensorPoller) events(ctx context.Context, id string, items []sensor.Item, received time.Time) ([]HazardEvent, int, error) {
	events := []HazardEvent{}
	unknown := 0

	for _, item := range items {
		o := item.Observation

		station, found, err := p.station(ctx, o.StationID)
		if err != nil {
			return nil, 0, err
		}

		if !found {
			if err = p.park(ctx, id, item.Raw, o.ID, "station_id is not listed by /v1/stations", received); err != nil {
				return nil, 0, err
			}

			unknown++

			continue
		}

		events = append(events, coastalEvent(o, station, received))
	}

	return events, unknown, nil
}

// station reloads the station list once when an observation names a station it has not seen.
func (p *SensorPoller) station(ctx context.Context, stationID string) (sensor.Station, bool, error) {
	if station, found := p.stations[stationID]; found {
		return station, true, nil
	}

	stations, err := p.Client.Stations(ctx)
	if err != nil {
		return sensor.Station{}, false, err
	}

	p.stations = stations
	station, found := stations[stationID]

	return station, found, nil
}

func coastalEvent(o sensor.Observation, station sensor.Station, received time.Time) HazardEvent {
	attributes := map[string]json.RawMessage{}
	for key, value := range map[string]any{
		"station_id": o.StationID, "province": station.Province, "sea_level_anomaly_m": o.SeaLevelAnomaly,
		"wave_height_m": o.WaveHeight, "quality": o.Quality, "critical": o.Critical,
	} {
		attributes[key], _ = json.Marshal(value)
	}

	return HazardEvent{
		HazardID: CanonicalHazardID(SensorSource, o.ID), Source: SensorSource, SourceRefID: o.ID, HazardType: "COASTAL",
		Severity: CoastalSeverity(o), AreaName: station.Name, Latitude: station.Latitude, Longitude: station.Longitude,
		OccurredAt: o.ObservedAt, IngestedAt: received, Attributes: attributes, Revision: 1,
	}
}

func (p *SensorPoller) commit(ctx context.Context, events []HazardEvent, offset int64, state sensorState, now time.Time) error {
	if p.Guard != nil {
		if err := p.Guard(ctx); err != nil {
			return err
		}
	}

	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode sensor state: %w", err)
	}

	batch := WriteBatch{Source: SensorSource, Epoch: p.Ingest.WriterEpoch, Events: events, Now: now}
	if _, err = p.Ingest.Data.CommitState(ctx, batch, StreamState{Offset: offset + 1, Payload: payload}); err != nil {
		return fmt.Errorf("commit sensor page: %w", err)
	}

	return nil
}

// report marks the source stale when the data looks sick, whatever /healthz says.
func (p *SensorPoller) report(ctx context.Context, id string, health sensor.Health, stored, parked int, now time.Time) {
	p.Log.Info("sensor_committed", "source", SensorSource, "correlation_id", id, "stored", stored, "parked", parked,
		"stale", health.Stale, "repeated", health.Repeated, "out_of_order", health.OutOfOrder)

	if !health.Degraded() {
		return
	}

	if err := p.Ingest.Failure(ctx, SensorSource, now); err != nil {
		p.Log.Warn("sensor_degraded_mark_failed", "source", SensorSource, "correlation_id", id, "reason", err.Error())
	}
}

func (p *SensorPoller) fail(ctx context.Context, id string, err error) {
	var contract *sensor.ContractError
	if errors.As(err, &contract) {
		if parkErr := p.park(ctx, id, contract.Raw, "", contract.Reason, time.Now().UTC()); parkErr != nil {
			p.Log.Warn("sensor_park_failed", "source", SensorSource, "correlation_id", id, "reason", parkErr.Error())
		}
	}

	markCtx, cancel := context.WithTimeout(ctx, defaultFailureMarkTimeout)
	defer cancel()

	markErr := p.Ingest.Failure(markCtx, SensorSource, time.Now().UTC())

	// While the breaker is open every poll fails the same way, and the state change was logged once.
	if !errors.Is(err, sensor.ErrOpen) {
		p.Log.Warn("poll_failed", "source", SensorSource, "correlation_id", id, "reason", err.Error(), "freshness_saved", markErr == nil)
	}
}

// park sets a record aside without retry, because a contract violation does not heal on replay.
func (p *SensorPoller) park(ctx context.Context, id string, raw []byte, ref, reason string, received time.Time) error {
	digest := sha256.Sum256(raw)
	identity := sha256.Sum256([]byte(SensorSource + "\x00" + hex.EncodeToString(digest[:])))

	return p.Ingest.Park(ctx, ParkedMessage{
		MessageID: hex.EncodeToString(identity[:]), Source: SensorSource, SourceRefID: ref, Raw: raw,
		ReasonCode: "contract_violation", Reason: reason, ReceivedAt: received, CorrelationID: id,
	})
}
