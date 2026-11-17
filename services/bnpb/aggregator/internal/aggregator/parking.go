package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type ParkedMessage struct {
	MessageID     string     `json:"message_id"`
	Source        string     `json:"source"`
	ReceivedAt    time.Time  `json:"received_at"`
	SourceRefID   string     `json:"source_ref_id,omitempty"`
	Raw           []byte     `json:"raw,omitempty"`
	RawLength     int64      `json:"raw_length"`
	RawSHA256     string     `json:"raw_sha256"`
	RawCut        bool       `json:"raw_cut"`
	ReasonCode    string     `json:"reason_code"`
	Reason        string     `json:"reason"`
	Retryable     bool       `json:"retryable"`
	Attempts      int        `json:"attempts"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	Status        string     `json:"status"`
	CorrelationID string     `json:"correlation_id"`
}
type ParkingPage struct {
	Items      []ParkedMessage `json:"items"`
	NextCursor *string         `json:"next_cursor"`
}
type Parking interface {
	Park(context.Context, ParkedMessage) error
	ListUnprocessed(context.Context, string, string, int) (ParkingPage, error)
	Replay(context.Context, string, int64, int, func(ParkedMessage) (HazardEvent, error)) (int, error)
	RetryDue(context.Context, string, int64, int, func(ParkedMessage) (HazardEvent, error)) (int, error)
}

const parkingColumns = `message_id,source,received_at,source_ref_id,raw,raw_length,raw_sha256,raw_cut,reason_code,reason,retryable,attempts,next_attempt_at,status,correlation_id`

func scanParked(row pgx.Row) (ParkedMessage, error) {
	var m ParkedMessage

	err := row.Scan(&m.MessageID, &m.Source, &m.ReceivedAt, &m.SourceRefID, &m.Raw, &m.RawLength, &m.RawSHA256, &m.RawCut, &m.ReasonCode, &m.Reason, &m.Retryable, &m.Attempts, &m.NextAttemptAt, &m.Status, &m.CorrelationID)

	return m, err
}

func (s *Store) Park(ctx context.Context, m ParkedMessage) error {
	if m.MessageID == "" || !validSource(m.Source) || m.ReceivedAt.IsZero() || m.ReasonCode == "" || m.Reason == "" {
		return errors.New("parking metadata required")
	}

	if len(m.Raw) > 4<<20 {
		return errors.New("oversized body requires bounded prefix")
	}

	if !m.RawCut {
		sum := sha256.Sum256(m.Raw)
		m.RawLength = int64(len(m.Raw))
		m.RawSHA256 = hex.EncodeToString(sum[:])
	} else if len(m.Raw) > 64<<10 || m.RawLength <= int64(len(m.Raw)) || len(m.RawSHA256) != 64 {
		return errors.New("invalid cut body metadata")
	}

	m.Status = "parked"
	if m.Retryable {
		m.Status = "retrying"
		next := m.ReceivedAt.Add(time.Second)
		m.NextAttemptAt = &next
	}

	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `INSERT INTO unprocessed_messages (`+parkingColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT(message_id) DO NOTHING`, m.MessageID, m.Source, m.ReceivedAt, m.SourceRefID, m.Raw, m.RawLength, m.RawSHA256, m.RawCut, m.ReasonCode, m.Reason, m.Retryable, m.Attempts, m.NextAttemptAt, m.Status, m.CorrelationID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) ListUnprocessed(ctx context.Context, source, cursor string, limit int) (ParkingPage, error) {
	page := ParkingPage{Items: []ParkedMessage{}}
	if limit < 1 || limit > 1000 {
		return page, errors.New("invalid limit")
	}

	var after struct {
		Time       time.Time
		ID, Source string
	}

	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || json.Unmarshal(raw, &after) != nil || after.ID == "" || after.Source != source {
			return page, errors.New("invalid parking cursor")
		}
	}

	tx, err := s.begin(ctx, true)
	if err != nil {
		return page, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `SELECT `+parkingColumns+` FROM unprocessed_messages WHERE status<>'recovered' AND ($1='' OR source=$1) AND ($4 OR (received_at,message_id)>($2,$3)) ORDER BY received_at,message_id LIMIT $5`, source, after.Time, after.ID, cursor == "", limit+1)
	if err != nil {
		return page, err
	}

	for rows.Next() {
		m, err := scanParked(rows)
		if err != nil {
			rows.Close()
			return page, err
		}

		page.Items = append(page.Items, m)
	}

	err = rows.Err()
	rows.Close()

	if err != nil {
		return page, err
	}

	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		last := page.Items[limit-1]
		after.Time = last.ReceivedAt
		after.ID = last.MessageID
		after.Source = source
		raw, _ := json.Marshal(after)
		next := base64.RawURLEncoding.EncodeToString(raw)
		page.NextCursor = &next
	}

	return page, tx.Commit(ctx)
}

// Replay serializes source commits and atomically recovers each ordered batch.
func (s *Store) Replay(ctx context.Context, source string, epoch int64, limit int, decode func(ParkedMessage) (HazardEvent, error)) (int, error) {
	return s.replay(ctx, source, epoch, limit, decode, false)
}

func (s *Store) RetryDue(ctx context.Context, source string, epoch int64, limit int, decode func(ParkedMessage) (HazardEvent, error)) (int, error) {
	return s.replay(ctx, source, epoch, limit, decode, true)
}

func (s *Store) replay(ctx context.Context, source string, epoch int64, limit int, decode func(ParkedMessage) (HazardEvent, error), dueOnly bool) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, errors.New("invalid limit")
	}

	tx, err := s.begin(ctx, false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var sealed int64
	if err = tx.QueryRow(ctx, `SELECT epoch FROM public.source_fence WHERE source=$1 FOR UPDATE`, source).Scan(&sealed); err != nil {
		return 0, err
	}

	if sealed != epoch {
		return 0, ErrStaleEpoch
	}

	rows, err := tx.Query(ctx, `SELECT `+parkingColumns+` FROM unprocessed_messages WHERE source=$1 AND status<>'recovered' ORDER BY received_at,message_id LIMIT $2 FOR UPDATE`, source, limit)
	if err != nil {
		return 0, err
	}

	pending := []ParkedMessage{}

	for rows.Next() {
		m, err := scanParked(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}

		pending = append(pending, m)
	}

	err = rows.Err()
	rows.Close()

	if err != nil {
		return 0, err
	}

	recovered := 0

	blocked := map[string]bool{}
	for _, m := range pending {
		if blocked[m.SourceRefID] {
			continue
		}

		if dueOnly && (m.Status != "retrying" || m.NextAttemptAt == nil || m.NextAttemptAt.After(time.Now())) {
			blocked[m.SourceRefID] = true
			continue
		}

		h, decodeErr := decode(m)
		if m.RawCut {
			decodeErr = errors.New("truncated input requires refetch")
		}

		if decodeErr != nil {
			attempts := m.Attempts + 1
			status := "parked"

			var next *time.Time

			if m.Retryable && attempts < 5 {
				status = "retrying"
				value := time.Now().UTC().Add(time.Second * time.Duration(1<<attempts))
				next = &value
			}

			if _, err = tx.Exec(ctx, `UPDATE unprocessed_messages SET attempts=$2,status=$3,next_attempt_at=$4 WHERE message_id=$1`, m.MessageID, attempts, status, next); err != nil {
				return 0, err
			}

			blocked[m.SourceRefID] = true

			continue
		}

		if h.Source != source || (m.SourceRefID != "" && h.SourceRefID != m.SourceRefID) {
			return 0, errors.New("replay identity mismatch")
		}

		var newest *time.Time
		if err = tx.QueryRow(ctx, `SELECT max(occurred_at) FROM hazard_events WHERE source=$1 AND source_ref_id=$2`, source, h.SourceRefID).Scan(&newest); err != nil {
			return 0, err
		}

		if newest == nil || !h.OccurredAt.Before(*newest) {
			if _, err = saveEvents(ctx, tx, source, []HazardEvent{h}, nil, time.Now().UTC()); err != nil {
				return 0, err
			}
		}

		if _, err = tx.Exec(ctx, `UPDATE unprocessed_messages SET status='recovered',attempts=attempts+1,next_attempt_at=NULL WHERE message_id=$1`, m.MessageID); err != nil {
			return 0, err
		}

		recovered++
	}

	return recovered, tx.Commit(ctx)
}

// RunParkingRetries is started by the source leadership owner and stopped on
// lease loss. Permanent errors remain parked until an explicit Replay request.
func RunParkingRetries(ctx context.Context, p Parking, source string, epoch int64, decode func(ParkedMessage) (HazardEvent, error), onError func(error)) {
	for ctx.Err() == nil {
		_, err := p.RetryDue(ctx, source, epoch, 100, decode)
		if errors.Is(err, ErrStaleEpoch) {
			onError(err)
			return
		}

		if err != nil {
			onError(err)
		}

		if !pause(ctx, time.Second) {
			return
		}
	}
}
