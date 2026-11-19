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
	ok := 0

	for _, copy := range t.Shards[shard] {
		if copy.State == "behind" {
			continue
		}

		n := c.Nodes[copy.Node]
		if n == nil {
			continue
		}

		call, cancel := c.timeout(ctx)
		err = n.Shards[shard].Park(call, m)

		cancel()

		if err == nil {
			ok++
		} else {
			return err
		}
	}

	if ok != 2 {
		return errors.New("parking requires both copies until degraded placement is fenced")
	}

	return nil
}

func (c *Cluster) ListUnprocessed(ctx context.Context, source, cursor string, limit int) (ParkingPage, error) {
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

	var cutoff *ParkedMessage

	for _, src := range requested {
		var page ParkingPage
		// Local cursor filters the same global timestamp/ID, but binds the source.
		local := ""

		if cursor != "" {
			value := after
			value.Source = src
			raw, _ := json.Marshal(value)
			local = base64.RawURLEncoding.EncodeToString(raw)
		}

		err := c.metadata(ctx, src, func(s *Store) error { var err error; page, err = s.ListUnprocessed(ctx, src, local, limit); return err })
		if err != nil {
			return out, err
		}

		out.Items = append(out.Items, page.Items...)
		if page.NextCursor != nil && len(page.Items) > 0 {
			last := page.Items[len(page.Items)-1]
			if cutoff == nil || last.ReceivedAt.Before(cutoff.ReceivedAt) || last.ReceivedAt.Equal(cutoff.ReceivedAt) && last.MessageID < cutoff.MessageID {
				cutoff = &last
			}
		}
	}

	sort.Slice(out.Items, func(i, j int) bool {
		a, b := out.Items[i], out.Items[j]
		if a.ReceivedAt.Equal(b.ReceivedAt) {
			return a.MessageID < b.MessageID
		}

		return a.ReceivedAt.Before(b.ReceivedAt)
	})

	if cutoff != nil {
		end := 0
		for end < len(out.Items) {
			message := out.Items[end]
			if message.ReceivedAt.After(cutoff.ReceivedAt) || message.ReceivedAt.Equal(cutoff.ReceivedAt) && message.MessageID > cutoff.MessageID {
				break
			}

			end++
		}

		out.Items = out.Items[:end]
	}

	more := len(out.Items) > limit
	if more {
		out.Items = out.Items[:limit]
	}
	// An exactly full merged page may still have more items; returning one extra
	// empty page is safe and avoids skipping per-source continuation.
	if len(out.Items) > 0 && (len(out.Items) == limit || cutoff != nil) {
		last := out.Items[len(out.Items)-1]
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

	var page ParkingPage

	if dueOnly {
		err = c.metadata(ctx, source, func(s *Store) error {
			var inner error

			page.Items, inner = s.dueParking(ctx, source, limit)

			return inner
		})
	} else {
		page, err = c.ListUnprocessed(ctx, source, "", limit)
	}

	if err != nil {
		return 0, err
	}

	recovered := 0

	blocked := map[string]bool{}
	for _, m := range page.Items {
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
	missing := false

	for _, copy := range t.Shards[shard] {
		if copy.State != "serving" || c.Nodes[copy.Node] == nil {
			continue
		}

		s := c.Nodes[copy.Node].Shards[shard]

		call, cancel := c.timeout(ctx)

		tx, err := s.begin(call, true)
		if err != nil {
			cancel()

			missing = true

			continue
		}

		h, err := scanEvent(tx.QueryRow(call, `SELECT `+columns+` FROM hazard_events WHERE hazard_id=$1`, id))
		_ = tx.Rollback(call)

		cancel()

		if err == nil {
			return h, nil
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return h, ErrNotFound
		}

		missing = true
	}

	if missing {
		return HazardEvent{}, errors.New("canonical copy unavailable")
	}

	return HazardEvent{}, ErrNotFound
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

		tx, err := n.Shards[shard].begin(ctx, false)
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
