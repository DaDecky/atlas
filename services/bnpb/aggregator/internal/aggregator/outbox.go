package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type Outbox interface {
	RelayOne(context.Context, EventPublisher) (bool, error)
}

func enqueueEvent(ctx context.Context, tx pgx.Tx, h HazardEvent, id string) error {
	raw, err := json.Marshal(h)
	if err != nil {
		return err
	}

	critical := false
	if h.Source == "SENSOR_PANTAI" {
		_ = json.Unmarshal(h.Attributes["critical"], &critical)
	}

	_, err = tx.Exec(ctx, `INSERT INTO public.publication_outbox(hazard_id,revision,payload,correlation_id,critical) VALUES($1,$2,$3::jsonb,$4,$5) ON CONFLICT DO NOTHING`, h.HazardID, h.Revision, string(raw), id, critical)

	return err
}

// RelayOne holds a SKIP LOCKED claim until confirmation and deletion commit.
// An uncertain outcome leaves the immutable row for at-least-once delivery.
func (s *Store) RelayOne(ctx context.Context, p EventPublisher) (bool, error) {
	return s.relayOne(ctx, p, false)
}

func (s *Store) relayOne(ctx context.Context, p EventPublisher, criticalOnly bool) (bool, error) {
	tx, err := s.ingest.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var (
		raw      []byte
		item     Publication
		critical bool
	)

	err = tx.QueryRow(ctx, `SELECT payload,correlation_id,critical FROM public.publication_outbox WHERE (NOT $1 OR critical) ORDER BY critical DESC,created_at,hazard_id,revision FOR UPDATE SKIP LOCKED LIMIT 1`, criticalOnly).Scan(&raw, &item.CorrelationID, &critical)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	if err = json.Unmarshal(raw, &item.Event); err != nil {
		return false, err
	}

	if critical {
		publisher, ok := p.(interface {
			PublishCritical(context.Context, Publication) error
		})
		if !ok {
			return false, errors.New("publisher has no critical lane")
		}

		if err = publisher.PublishCritical(ctx, item); err != nil {
			return false, err
		}
	}

	if err = p.Publish(ctx, item); err != nil {
		return false, err
	}

	if _, err = tx.Exec(ctx, `DELETE FROM public.publication_outbox WHERE hazard_id=$1 AND revision=$2`, item.Event.HazardID, item.Event.Revision); err != nil {
		return false, err
	}

	return true, tx.Commit(ctx)
}

func RunRelay(ctx context.Context, s Outbox, p EventPublisher, interval time.Duration, onError func(error)) {
	defer p.Close()

	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		sent, err := s.RelayOne(call, p)

		cancel()

		if err != nil {
			onError(err)
		}

		if sent && err == nil {
			continue
		}

		if !pause(ctx, interval) {
			return
		}
	}
}
