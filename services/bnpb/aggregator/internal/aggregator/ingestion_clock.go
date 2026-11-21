package aggregator

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
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

	existing, err := c.existingIngestedAt(ctx, b.Events)
	if err != nil {
		return err
	}

	b.Events = append([]HazardEvent(nil), b.Events...)
	for i, h := range b.Events {
		if at, found := existing[h.HazardID]; found {
			b.Events[i].IngestedAt = at
			continue
		}

		value := h.IngestedAt.UTC()
		if !value.After(last) {
			value = last.Add(time.Millisecond)
		}

		b.Events[i].IngestedAt = value
		last = value
	}

	// The next batch reads a majority, so a majority must hold the new value or time could go back.
	updated := 0

	for _, node := range c.Nodes {
		call, cancel := c.timeout(ctx)
		_, err := node.Store.ingest.Exec(call, `UPDATE public.ingestion_clock SET last_value=GREATEST(last_value,$1) WHERE singleton`, last)

		cancel()

		if err == nil {
			updated++
		}
	}

	if updated < len(c.Nodes)/2+1 {
		return errors.New("ingestion clock update majority unavailable")
	}

	b.timestampsAssigned = true

	return nil
}

// existingIngestedAt reads the stored ingestion time of each event that already exists, one query per shard.
func (c *Cluster) existingIngestedAt(ctx context.Context, events []HazardEvent) (map[string]time.Time, error) {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return nil, err
	}

	byShard := map[string][]string{}

	for _, h := range events {
		shard := placement.Shard(h.HazardID)
		byShard[shard] = append(byShard[shard], h.HazardID)
	}

	found := map[string]time.Time{}

	for shard, ids := range byShard {
		times, err := readCopies(ctx, c, t.Shards[shard], shard, 0, func(call context.Context, s *Store) (map[string]time.Time, error) {
			return s.ingestedAt(call, ids)
		})
		if err != nil {
			return nil, err
		}

		for id, at := range times {
			found[id] = at
		}
	}

	return found, nil
}

func (s *Store) ingestedAt(ctx context.Context, ids []string) (map[string]time.Time, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `SELECT hazard_id, ingested_at FROM hazard_events WHERE hazard_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	times := map[string]time.Time{}

	for rows.Next() {
		var (
			id string
			at time.Time
		)

		if err = rows.Scan(&id, &at); err != nil {
			return nil, err
		}

		times[id] = at
	}

	return times, rows.Err()
}
