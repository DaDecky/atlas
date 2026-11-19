package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
)

// Field reports are operator observations, not a fifth HazardEvent source.
// The immutable inbox row is the applied report and its idempotency receipt in
// one transaction. Reusing an ID preserves its first stored observation.
type FieldReport struct {
	ReportID   string    `json:"report_id"`
	ObservedAt time.Time `json:"observed_at"`
	Location   struct {
		Latitude  *float64 `json:"latitude"`
		Longitude *float64 `json:"longitude"`
		AreaName  string   `json:"area_name"`
	} `json:"location"`
	Category string `json:"category"`
	Message  string `json:"message"`
	ClientID string `json:"client_id"`
}

type FieldReportStore interface {
	ApplyFieldReport(context.Context, FieldReport) (FieldReport, bool, error)
}

func DecodeFieldReport(raw []byte) (FieldReport, error) {
	var report FieldReport
	if len(raw) > 16<<10 || !utf8.Valid(raw) {
		return report, errors.New("invalid report size or encoding")
	}

	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()

	if err := d.Decode(&report); err != nil {
		return report, errors.New("invalid report JSON")
	}

	var extra any
	if d.Decode(&extra) != io.EOF || report.ReportID == "" || len(report.ReportID) > 128 || report.ObservedAt.IsZero() ||
		report.Location.Latitude == nil || report.Location.Longitude == nil ||
		math.IsNaN(*report.Location.Latitude) || math.Abs(*report.Location.Latitude) > 90 || math.Abs(*report.Location.Longitude) > 180 ||
		strings.TrimSpace(report.Location.AreaName) == "" || len(report.Location.AreaName) > 256 ||
		strings.TrimSpace(report.Message) == "" || len(report.Message) > 4096 || report.ClientID == "" || len(report.ClientID) > 256 {
		return report, errors.New("invalid report fields")
	}

	for _, c := range report.ReportID {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("-_.", c)) {
			return report, errors.New("invalid report ID")
		}
	}

	switch report.Category {
	case "SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL":
	default:
		return report, errors.New("invalid report category")
	}

	return report, nil
}

func (s *Store) ApplyFieldReport(ctx context.Context, report FieldReport) (FieldReport, bool, error) {
	raw, err := json.Marshal(report)
	if err != nil {
		return FieldReport{}, false, err
	}

	if _, err = DecodeFieldReport(raw); err != nil {
		return FieldReport{}, false, err
	}

	tx, err := s.begin(ctx, false)
	if err != nil {
		return FieldReport{}, false, err
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, `INSERT INTO field_reports_inbox(report_id,payload,received_at) VALUES($1,$2::jsonb,$3) ON CONFLICT(report_id) DO NOTHING`, report.ReportID, string(raw), time.Now().UTC())
	if err != nil {
		return FieldReport{}, false, err
	}

	if err = tx.QueryRow(ctx, `SELECT payload FROM field_reports_inbox WHERE report_id=$1`, report.ReportID).Scan(&raw); err != nil {
		return FieldReport{}, false, err
	}

	stored, err := DecodeFieldReport(raw)
	if err != nil {
		return FieldReport{}, false, err
	}

	return stored, result.RowsAffected() == 1, tx.Commit(ctx)
}

func (c *Cluster) ApplyFieldReport(ctx context.Context, report FieldReport) (FieldReport, bool, error) {
	table, version, err := c.Placement.Load(ctx)
	if err != nil {
		return FieldReport{}, false, err
	}

	shard := placement.Shard("field-report:" + report.ReportID)

	copies := table.Shards[shard]
	if len(copies) != 2 || copies[0].State != "serving" || copies[1].State != "serving" {
		return FieldReport{}, false, errors.New("field report requires two serving copies")
	}
	// A newly promoted primary may be missing an in-flight, unacknowledged row
	// from an older placement. Prefer a durable receipt on either current copy
	// before inserting, so an inclusive broker replay repairs that gap.
	for _, copy := range copies {
		node := c.Nodes[copy.Node]
		if node == nil || node.Shards[shard] == nil {
			return FieldReport{}, false, errors.New("field report holder unavailable")
		}

		call, cancel := c.timeout(ctx)
		stored, found, readErr := node.Shards[shard].fieldReport(call, report.ReportID)

		cancel()

		if readErr != nil {
			return FieldReport{}, false, readErr
		}

		if found {
			report = stored
			break
		}
	}
	// Every consumer first visits the same primary. Its unique row chooses the
	// winning immutable payload even when concurrent deliveries differ. A crash
	// between copies is repaired by redelivery before acknowledging the queue.
	inserted := false

	for i, copy := range copies {
		node := c.Nodes[copy.Node]
		if node == nil || node.Shards[shard] == nil {
			return FieldReport{}, false, errors.New("field report holder unavailable")
		}

		call, cancel := c.timeout(ctx)
		stored, created, err := node.Shards[shard].ApplyFieldReport(call, report)

		cancel()

		if err != nil {
			return FieldReport{}, false, err
		}

		if i == 0 {
			report, inserted = stored, created
		} else {
			a, _ := json.Marshal(report)

			b, _ := json.Marshal(stored)
			if !bytes.Equal(a, b) {
				return FieldReport{}, false, errors.New("field report copy conflicts with primary")
			}
		}
	}

	_, currentVersion, err := c.Placement.Load(ctx)
	if err != nil {
		return FieldReport{}, false, err
	}

	if currentVersion != version {
		return FieldReport{}, false, errors.New("field report placement changed; redelivery required")
	}

	return report, inserted, nil
}

func (s *Store) fieldReport(ctx context.Context, id string) (FieldReport, bool, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return FieldReport{}, false, err
	}
	defer tx.Rollback(ctx)

	var raw []byte

	err = tx.QueryRow(ctx, `SELECT payload FROM field_reports_inbox WHERE report_id=$1`, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return FieldReport{}, false, nil
	}

	if err != nil {
		return FieldReport{}, false, err
	}

	report, err := DecodeFieldReport(raw)

	return report, true, err
}

func (s *Store) restoreFieldReport(ctx context.Context, id string, payload []byte, received time.Time) error {
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `INSERT INTO field_reports_inbox(report_id,payload,received_at) VALUES($1,$2::jsonb,$3) ON CONFLICT(report_id) DO NOTHING`, id, string(payload), received)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
