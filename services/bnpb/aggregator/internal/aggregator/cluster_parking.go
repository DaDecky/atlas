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

// parkingCursor is the global position of a merged page, bound to the requested source.
type parkingCursor struct {
	Time       time.Time
	ID, Source string
}

// Parking uses the shard of its metadata key, one of the 48 data shards, so it has the
// same two-copy placement and refill lifecycle as source state.
// Recovery marks follow idempotent writes.
func (c *Cluster) Park(ctx context.Context, m ParkedMessage) error {
	return c.park(ctx, m, 0)
}

func (c *Cluster) park(ctx context.Context, m ParkedMessage, epoch int64) error {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	if epoch == 0 {
		epoch = t.WriterEpochs[m.Source]
	}

	if epoch != t.WriterEpochs[m.Source] {
		return ErrStaleEpoch
	}

	shard := placement.Shard("metadata:" + m.Source)
	_, err = c.replicate(ctx, t, shard, m.Source, epoch, func(call context.Context, s *Store) ([]HazardEvent, error) {
		return nil, s.park(call, m, epoch)
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

	var after parkingCursor

	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || json.Unmarshal(raw, &after) != nil || after.ID == "" || after.Source != source {
			return out, errors.New("invalid cursor")
		}
	}

	items, cutoff, err := c.sourceParking(ctx, source, cursor, after, limit)
	if err != nil {
		return out, err
	}

	if publications {
		items = append(items, c.publicationParking(ctx, source, cursor, limit)...)
	}

	out.Items = cutParking(mergeParking(items), cutoff)

	if len(out.Items) > limit {
		out.Items = out.Items[:limit]
	}
	// An exactly full merged page may still have more items; returning one extra
	// empty page is safe and avoids skipping per-source continuation.
	if len(out.Items) > 0 && (len(out.Items) == limit || cutoff != nil) {
		last := out.Items[len(out.Items)-1]
		raw, _ := json.Marshal(parkingCursor{Time: last.ReceivedAt, ID: last.MessageID, Source: source})
		next := base64.RawURLEncoding.EncodeToString(raw)
		out.NextCursor = &next
	}

	return out, nil
}

// sourceParking reads one page per source and returns the earliest last item of a page
// that has more, since nothing after it is complete in the merged page.
func (c *Cluster) sourceParking(ctx context.Context, source, cursor string, after parkingCursor, limit int) ([]ParkedMessage, *ParkedMessage, error) {
	requested := []string{source}
	if source == "" {
		requested = Sources
	}

	var (
		items  []ParkedMessage
		cutoff *ParkedMessage
	)

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
			return nil, nil, err
		}

		items = append(items, page.Items...)

		if page.NextCursor != nil && len(page.Items) > 0 {
			last := page.Items[len(page.Items)-1]
			if cutoff == nil || parkedBefore(last, *cutoff) {
				cutoff = &last
			}
		}
	}

	return items, cutoff, nil
}

// publicationParking reads publication failures, which live beside each node's outbox,
// not in source shards. An unreachable node is skipped.
func (c *Cluster) publicationParking(ctx context.Context, source, cursor string, limit int) []ParkedMessage {
	var items []ParkedMessage

	for _, node := range c.Nodes {
		call, cancel := c.timeout(ctx)
		page, err := node.Store.ListUnprocessed(call, source, cursor, limit)

		cancel()

		if err == nil {
			items = append(items, page.Items...)
		}
	}

	return items
}

// mergeParking keeps the copy of each message with the most attempts, in received order.
func mergeParking(items []ParkedMessage) []ParkedMessage {
	dedup := map[string]ParkedMessage{}

	for _, m := range items {
		old, exists := dedup[m.MessageID]
		if !exists || m.Attempts > old.Attempts {
			dedup[m.MessageID] = m
		}
	}

	merged := make([]ParkedMessage, 0, len(dedup))
	for _, m := range dedup {
		merged = append(merged, m)
	}

	sort.Slice(merged, func(i, j int) bool { return parkedBefore(merged[i], merged[j]) })

	return merged
}

// cutParking drops every item after cutoff.
func cutParking(items []ParkedMessage, cutoff *ParkedMessage) []ParkedMessage {
	if cutoff == nil {
		return items
	}

	end := 0
	for end < len(items) && !parkedBefore(*cutoff, items[end]) {
		end++
	}

	return items[:end]
}

func parkedBefore(a, b ParkedMessage) bool {
	if a.ReceivedAt.Equal(b.ReceivedAt) {
		return a.MessageID < b.MessageID
	}

	return a.ReceivedAt.Before(b.ReceivedAt)
}

func (c *Cluster) Replay(ctx context.Context, source string, epoch int64, limit int, decode func(ParkedMessage) (HazardEvent, error)) (int, error) {
	return c.ReplayReason(ctx, source, epoch, limit, "", decode)
}

func (c *Cluster) ReplayReason(ctx context.Context, source string, epoch int64, limit int, reason string, decode func(ParkedMessage) (HazardEvent, error)) (int, error) {
	return c.replay(ctx, source, epoch, limit, reason, decode, false)
}

func (c *Cluster) RetryDue(ctx context.Context, source string, epoch int64, limit int, decode func(ParkedMessage) (HazardEvent, error)) (int, error) {
	return c.replay(ctx, source, epoch, limit, "", decode, true)
}

func (c *Cluster) replay(ctx context.Context, source string, epoch int64, limit int, reason string, decode func(ParkedMessage) (HazardEvent, error), dueOnly bool) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, errors.New("invalid limit")
	}

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

	items, err := c.replayItems(ctx, source, limit, reason, dueOnly)
	if err != nil {
		return 0, err
	}

	recovered := 0
	blocked := map[string]bool{}

	for _, m := range items {
		if m.ReasonCode == "publish_failed" || blocked[m.SourceRefID] {
			continue
		}

		if dueOnly && !replayDue(m, time.Now()) {
			blocked[m.SourceRefID] = true
			continue
		}

		ok, err := c.replayOne(ctx, source, epoch, m, decode)
		if err != nil {
			return recovered, err
		}

		if !ok {
			blocked[m.SourceRefID] = true
			continue
		}

		recovered++
	}

	return recovered, nil
}

func (c *Cluster) replayItems(ctx context.Context, source string, limit int, reason string, dueOnly bool) ([]ParkedMessage, error) {
	if dueOnly || reason != "" {
		return readMetadata(ctx, c, source, func(call context.Context, s *Store) ([]ParkedMessage, error) {
			return s.replayCandidates(call, source, limit, reason, dueOnly)
		})
	}

	page, err := c.listUnprocessed(ctx, source, "", limit, false)

	return page.Items, err
}

// replayOne applies one parked message and reports whether it recovered. A message that
// still fails is marked failed and blocks the later messages of the same event.
func (c *Cluster) replayOne(ctx context.Context, source string, epoch int64, m ParkedMessage, decode func(ParkedMessage) (HazardEvent, error)) (bool, error) {
	h, err := decode(m)
	if m.RawCut {
		err = errors.New("truncated message requires refetch")
	}

	if err != nil {
		return false, c.markParking(ctx, source, epoch, m, false)
	}

	if h.Source != source || (m.SourceRefID != "" && m.SourceRefID != h.SourceRefID) {
		return false, errors.New("replay identity mismatch")
	}
	// Inspect the current canonical version before replaying an older message.
	fresh, err := c.event(ctx, h.HazardID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return false, err
	}

	if errors.Is(err, ErrNotFound) || !h.OccurredAt.Before(fresh.OccurredAt) {
		if _, err = c.write(ctx, WriteBatch{Source: source, Epoch: epoch, Now: time.Now().UTC(), Events: []HazardEvent{h}, sourceLocked: true}, nil, nil); err != nil {
			return false, err
		}
	}

	return true, c.markParking(ctx, source, epoch, m, true)
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
	for _, holder := range t.Shards[shard] {
		if holder.State == placement.Behind {
			continue
		}

		n := c.Nodes[holder.Node]
		if n == nil {
			return errors.New("parking copy unavailable")
		}

		tx, err := c.copyStore(holder, shard).begin(ctx, false)
		if err != nil {
			return err
		}

		var current int64

		err = tx.QueryRow(ctx, `SELECT epoch FROM public.source_fence WHERE source=$1 FOR UPDATE`, source).Scan(&current)
		if err == nil && current != epoch {
			err = ErrStaleEpoch
		}

		if err == nil {
			status, next := nextParkingStatus(m.Retryable, !recovered, m.Attempts+1, time.Now().UTC())

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
