package aggregator

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func normalizeEvents(ctx context.Context, tx pgx.Tx, events []HazardEvent) error {
	var last time.Time
	if err := tx.QueryRow(ctx, `SELECT last_value FROM public.ingestion_clock WHERE singleton FOR UPDATE`).Scan(&last); err != nil {
		return err
	}

	for i, h := range events {
		var original time.Time

		err := tx.QueryRow(ctx, `SELECT ingested_at FROM hazard_events WHERE source=$1 AND source_ref_id=$2`, h.Source, h.SourceRefID).Scan(&original)
		if err == nil {
			events[i].IngestedAt = original
			continue
		}

		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		next := h.IngestedAt.UTC()
		if !next.After(last) {
			next = last.Add(time.Millisecond)
		}

		events[i].IngestedAt = next
		last = next
	}

	_, err := tx.Exec(ctx, `UPDATE public.ingestion_clock SET last_value=GREATEST(last_value,$1) WHERE singleton`, last)

	return err
}

// Assign once before dual writes, otherwise clocks on two stores could produce
// different canonical timestamps. A majority read covers one unavailable node.
func (c *Cluster) assignIngestionTimes(ctx context.Context, b *WriteBatch) error {
	if len(b.Events) == 0 {
		b.timestampsAssigned = true
		return nil
	}

	var last time.Time

	count := 0

	for _, node := range c.Nodes {
		call, cancel := c.timeout(ctx)

		var value time.Time

		err := node.Store.query.QueryRow(call, `SELECT last_value FROM public.ingestion_clock WHERE singleton`).Scan(&value)

		cancel()

		if err == nil {
			count++

			if value.After(last) {
				last = value
			}
		}
	}

	if count < len(c.Nodes)/2+1 {
		return errors.New("ingestion clock majority unavailable")
	}

	b.Events = append([]HazardEvent(nil), b.Events...)
	for i, h := range b.Events {
		old, err := c.event(ctx, h.HazardID)
		if err == nil {
			b.Events[i].IngestedAt = old.IngestedAt
			continue
		}

		if !errors.Is(err, ErrNotFound) {
			return err
		}

		value := h.IngestedAt.UTC()
		if !value.After(last) {
			value = last.Add(time.Millisecond)
		}

		b.Events[i].IngestedAt = value
		last = value
	}

	for _, node := range c.Nodes {
		call, cancel := c.timeout(ctx)
		_, err := node.Store.ingest.Exec(call, `UPDATE public.ingestion_clock SET last_value=GREATEST(last_value,$1) WHERE singleton`, last)

		cancel()

		if err != nil {
			continue
		}
	}

	b.timestampsAssigned = true

	return nil
}
