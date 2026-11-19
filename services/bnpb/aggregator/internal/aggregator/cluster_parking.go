package aggregator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
)

// Parking uses reserved metadata shards so it has the same two-copy placement
// and refill lifecycle as source state. Recovery marks follow idempotent writes.
func (c *Cluster) Park(ctx context.Context, m ParkedMessage) error {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	shard := placement.Shard("metadata:" + m.Source)
	_, err = c.replicate(ctx, t, shard, m.Source, t.WriterEpochs[m.Source], func(call context.Context, s *Store) ([]HazardEvent, error) {
		return nil, s.Park(call, m)
	})

	return err
}

func (c *Cluster) ListUnprocessed(ctx context.Context, source, cursor string, limit int) (ParkingPage, error) {
	return c.listUnprocessed(ctx, source, cursor, limit, true)
}

func (c *Cluster) listUnprocessed(ctx context.Context, source, cursor string, limit int, publications bool) (ParkingPage, error) {
	out := ParkingPage{Items: []ParkedMessage{}}
	if limit < 1 || limit > 1000 {
		return out, errors.New("invalid limit")
	}

	var after struct {
		Time       time.Time
		ID, Source string
	}

	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || json.Unmarshal(raw, &after) != nil || after.ID == "" || after.Source != source {
			return out, errors.New("invalid cursor")
		}
	}

	requested := []string{source}
	if source == "" {
		requested = []string{"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"}
	}

	for _, src := range requested {
		// Local cursor filters the same global timestamp/ID, but binds the source.
		local := ""

		if cursor != "" {
			value := after
			value.Source = src
			raw, _ := json.Marshal(value)
			local = base64.RawURLEncoding.EncodeToString(raw)
		}

		page, err := readMetadata(ctx, c, src, func(call context.Context, s *Store) (ParkingPage, error) {
			return s.ListUnprocessed(call, src, local, limit)
		})
		if err != nil {
			return out, err
		}

		out.Items = append(out.Items, page.Items...)
	}

	if publications {
		// Publication failures live beside each node's outbox, not in source shards.
		for _, node := range c.Nodes {
			call, cancel := c.timeout(ctx)
			page, err := node.Store.ListUnprocessed(call, source, cursor, limit)

			cancel()

			if err != nil {
				continue
			}

			out.Items = append(out.Items, page.Items...)
		}
	}

	dedup := map[string]ParkedMessage{}
	for _, m := range out.Items {
		old, exists := dedup[m.MessageID]
		if !exists || m.Attempts > old.Attempts {
			dedup[m.MessageID] = m
		}
	}

	out.Items = out.Items[:0]
	for _, m := range dedup {
		out.Items = append(out.Items, m)
	}

	sort.Slice(out.Items, func(i, j int) bool {
		a, b := out.Items[i], out.Items[j]
		if a.ReceivedAt.Equal(b.ReceivedAt) {
			return a.MessageID < b.MessageID
		}

		return a.ReceivedAt.Before(b.ReceivedAt)
	})

	more := len(out.Items) > limit
	if more {
		out.Items = out.Items[:limit]
	}
	// An exactly full merged page may still have more items; returning one extra
	// empty page is safe and avoids skipping per-source continuation.
	if len(out.Items) == limit {
		last := out.Items[limit-1]
		after.Time = last.ReceivedAt
		after.ID = last.MessageID
		after.Source = source
		raw, _ := json.Marshal(after)
		next := base64.RawURLEncoding.EncodeToString(raw)
		out.NextCursor = &next
	}

	return out, nil
}

func (c *Cluster) Replay(ctx context.Context, source string, epoch int64, limit int, decode func(ParkedMessage) (HazardEvent, error)) (int, error) {
	return c.replay(ctx, source, epoch, limit, decode, false)
}

func (c *Cluster) RetryDue(ctx context.Context, source string, epoch int64, limit int, decode func(ParkedMessage) (HazardEvent, error)) (int, error) {
	return c.replay(ctx, source, epoch, limit, decode, true)
}

func (c *Cluster) replay(ctx context.Context, source string, epoch int64, limit int, decode func(ParkedMessage) (HazardEvent, error), dueOnly bool) (int, error) {
	release, err := c.lockSource(ctx, source)
	if err != nil {
		return 0, err
	}
	defer release()

	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return 0, err
	}

	if t.WriterEpochs[source] != epoch {
		return 0, ErrStaleEpoch
	}

	page, err := c.listUnprocessed(ctx, source, "", limit, false)
	if err != nil {
		return 0, err
	}

	recovered := 0

	blocked := map[string]bool{}

	for _, m := range page.Items {
		if m.ReasonCode == "publish_failed" {
			continue
		}

		if blocked[m.SourceRefID] {
			continue
		}

		if dueOnly && (m.Status != "retrying" || m.NextAttemptAt == nil || m.NextAttemptAt.After(time.Now())) {
			blocked[m.SourceRefID] = true
			continue
		}

		h, decodeErr := decode(m)
		if m.RawCut {
			decodeErr = errors.New("truncated message requires refetch")
		}

		if decodeErr != nil {
			blocked[m.SourceRefID] = true
			if err = c.markParking(ctx, source, epoch, m, false); err != nil {
				return recovered, err
			}

			continue
		}

		if h.Source != source || (m.SourceRefID != "" && m.SourceRefID != h.SourceRefID) {
			return recovered, errors.New("replay identity mismatch")
		}
		// Inspect the current canonical version before replaying an older message.
		fresh, err := c.event(ctx, h.HazardID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return recovered, err
		}

		if errors.Is(err, ErrNotFound) || !h.OccurredAt.Before(fresh.OccurredAt) {
			if _, err = c.write(ctx, WriteBatch{Source: source, Epoch: epoch, Now: time.Now().UTC(), Events: []HazardEvent{h}, sourceLocked: true}, nil, nil); err != nil {
				return recovered, err
			}
		}

		if err = c.markParking(ctx, source, epoch, m, true); err != nil {
			return recovered, err
		}

		recovered++
	}

	return recovered, nil
}

func (c *Cluster) event(ctx context.Context, id string) (HazardEvent, error) {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return HazardEvent{}, err
	}

	shard := placement.Shard(id)

	event, err := readCopies(ctx, c, t.Shards[shard], shard, func(call context.Context, s *Store) (*HazardEvent, error) {
		tx, err := s.begin(call, true)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(call)

		h, err := scanEvent(tx.QueryRow(call, `SELECT `+columns+` FROM hazard_events WHERE hazard_id=$1`, id))
		// A healthy absence completes the lookup; a slow second copy cannot turn it
		// into an outage or prevent ingestion of a genuinely new event.
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return &h, err
	})
	if err != nil {
		return HazardEvent{}, err
	}

	if event == nil {
		return HazardEvent{}, ErrNotFound
	}

	return *event, nil
}

func (c *Cluster) markParking(ctx context.Context, source string, epoch int64, m ParkedMessage, recovered bool) error {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	if t.WriterEpochs[source] != epoch {
		return ErrStaleEpoch
	}

	shard := placement.Shard("metadata:" + source)
	for _, copy := range t.Shards[shard] {
		if copy.State == "behind" {
			continue
		}

		n := c.Nodes[copy.Node]
		if n == nil {
			return errors.New("parking copy unavailable")
		}

		tx, err := c.copyStore(copy, shard).begin(ctx, false)
		if err != nil {
			return err
		}

		var current int64

		err = tx.QueryRow(ctx, `SELECT epoch FROM public.source_fence WHERE source=$1 FOR UPDATE`, source).Scan(&current)
		if err == nil && current != epoch {
			err = ErrStaleEpoch
		}

		if err == nil {
			status := "recovered"

			var next *time.Time

			if !recovered {
				status = "parked"
				if m.Retryable && m.Attempts+1 < 5 {
					status = "retrying"
					v := time.Now().UTC().Add(time.Second * time.Duration(1<<(m.Attempts+1)))
					next = &v
				}
			}

			_, err = tx.Exec(ctx, `UPDATE unprocessed_messages SET status=$2,attempts=attempts+1,next_attempt_at=$3 WHERE message_id=$1 AND status<>'recovered'`, m.MessageID, status, next)
		}

		if err == nil {
			err = tx.Commit(ctx)
		}

		_ = tx.Rollback(ctx)

		if err != nil {
			return err
		}
	}

	return nil
}
