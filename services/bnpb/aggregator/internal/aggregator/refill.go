package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"atlas/services/bnpb/aggregator/internal/placement"
)

// Refill must be called by the holder of the placement Lease. It first enables
// dual writes to the target without serving reads from it. Revision-conditional
// copy cannot overwrite a newer live write. A failed live write changes the
// placement version, so the final CAS cannot promote that failed copy.
func (c *Cluster) Refill(ctx context.Context, shard, target string) error {
	t, v, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	var from string

	found := false

	for i, copy := range t.Shards[shard] {
		if copy.Node == target {
			found = true
			t.Shards[shard][i].State = "refilling"
		} else if copy.State == "serving" {
			from = copy.Node
		}
	}

	if !found || from == "" || c.Nodes[target] == nil || c.Nodes[from] == nil {
		return errors.New("refill needs a serving source and configured target")
	}

	t.Generation++
	if err = c.Placement.CompareAndSwap(ctx, v, t); err != nil {
		return err
	}

	t, v, err = c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	source, dest := c.Nodes[from].Shards[shard], c.Nodes[target].Shards[shard]
	if err = copyShard(ctx, source, dest); err != nil {
		return err
	}
	// Concurrent dual writes can make two snapshots differ transiently. Retry,
	// leaving the target non-serving if verification cannot establish equality.
	verified := false

	for range 5 {
		a, err := shardDigest(ctx, source)
		if err != nil {
			return err
		}

		b, err := shardDigest(ctx, dest)
		if err != nil {
			return err
		}

		if a == b {
			verified = true
			break
		}

		if err = copyShard(ctx, source, dest); err != nil {
			return err
		}
	}

	if !verified {
		return errors.New("refill checksum differs; target remains non-serving")
	}

	for i := range t.Shards[shard] {
		if t.Shards[shard][i].Node == target {
			t.Shards[shard][i].State = "serving"
		}
	}

	t.Generation++

	return c.Placement.CompareAndSwap(ctx, v, t)
}

func copyShard(ctx context.Context, from, to *Store) error {
	tx, err := from.begin(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `SELECT `+columns+` FROM hazard_events ORDER BY hazard_id`)
	if err != nil {
		return err
	}

	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			rows.Close()
			return err
		}

		if err = to.restoreEvent(ctx, event); err != nil {
			rows.Close()
			return err
		}
	}

	err = rows.Err()
	rows.Close()

	if err != nil {
		return err
	}
	// State and checkpoint use the same snapshot as the copied events. Updates on
	// the target keep the greater offset/checkpoint if a live commit raced refill.
	stateRows, err := tx.Query(ctx, `SELECT source,committed_offset,payload FROM stream_state`)
	if err != nil {
		return err
	}

	for stateRows.Next() {
		var (
			source string
			offset int64
			raw    []byte
		)
		if err = stateRows.Scan(&source, &offset, &raw); err != nil {
			stateRows.Close()
			return err
		}

		if err = to.restoreState(ctx, source, offset, raw); err != nil {
			stateRows.Close()
			return err
		}
	}

	err = stateRows.Err()
	stateRows.Close()

	if err != nil {
		return err
	}

	parkingRows, err := tx.Query(ctx, `SELECT `+parkingColumns+` FROM unprocessed_messages ORDER BY message_id`)
	if err != nil {
		return err
	}

	for parkingRows.Next() {
		m, err := scanParked(parkingRows)
		if err != nil {
			parkingRows.Close()
			return err
		}

		if err = to.restoreParked(ctx, m); err != nil {
			parkingRows.Close()
			return err
		}
	}

	err = parkingRows.Err()
	parkingRows.Close()

	if err != nil {
		return err
	}

	for _, source := range []string{"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"} {
		var (
			progress   Freshness
			checkpoint *time.Time
		)
		if err = tx.QueryRow(ctx, `SELECT checkpoint,last_success_at,stale_since FROM source_progress WHERE source=$1`, source).Scan(&checkpoint, &progress.LastSuccessAt, &progress.StaleSince); err != nil {
			return err
		}

		target, err := to.begin(ctx, false)
		if err != nil {
			return err
		}

		_, err = target.Exec(ctx, `UPDATE source_progress SET checkpoint=GREATEST(checkpoint,$2),last_success_at=GREATEST(last_success_at,$3),stale_since=CASE WHEN last_success_at IS NULL OR last_success_at <= $3 THEN $4 ELSE stale_since END WHERE source=$1`, source, checkpoint, progress.LastSuccessAt, progress.StaleSince)
		if err == nil {
			err = target.Commit(ctx)
		}

		_ = target.Rollback(ctx)

		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) restoreEvent(ctx context.Context, h HazardEvent) error {
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	raw, err := json.Marshal(h.Attributes)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `INSERT INTO hazard_events (`+columns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
 ON CONFLICT (hazard_id) DO UPDATE SET severity=EXCLUDED.severity,area_name=EXCLUDED.area_name,
 latitude=EXCLUDED.latitude,longitude=EXCLUDED.longitude,occurred_at=EXCLUDED.occurred_at,
 attributes=EXCLUDED.attributes,revision=EXCLUDED.revision
 WHERE hazard_events.revision < EXCLUDED.revision`, h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, string(raw), h.Revision)
	if err != nil {
		return err
	}

	if err = enqueueEvent(ctx, tx, h, correlationID()); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) restoreState(ctx context.Context, source string, offset int64, raw []byte) error {
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `INSERT INTO stream_state(source,committed_offset,payload) VALUES($1,$2,$3::jsonb)
 ON CONFLICT(source) DO UPDATE SET committed_offset=EXCLUDED.committed_offset,payload=EXCLUDED.payload
 WHERE stream_state.committed_offset < EXCLUDED.committed_offset`, source, offset, string(raw))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func shardDigest(ctx context.Context, s *Store) (string, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `SELECT row_to_json(h)::text FROM (SELECT `+columns+` FROM hazard_events ORDER BY hazard_id COLLATE "C") h`)
	if err != nil {
		return "", err
	}

	hash := sha256.New()

	for rows.Next() {
		var raw string
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return "", err
		}

		_, _ = fmt.Fprintln(hash, raw)
	}

	err = rows.Err()
	rows.Close()

	if err != nil {
		return "", err
	}

	if err = tx.Commit(ctx); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// MoveCopy replaces one holder while retaining a serving donor. The placement
// lease holder can use this for a replacement node or incremental rebalance.
func (c *Cluster) MoveCopy(ctx context.Context, shard, old, newNode string) error {
	t, v, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	if c.Nodes[newNode] == nil {
		return errors.New("new node is not configured")
	}

	donor := false

	for _, copy := range t.Shards[shard] {
		if copy.Node != old && copy.State == "serving" && c.Nodes[copy.Node] != nil {
			call, cancel := c.timeout(ctx)
			_, err := c.Nodes[copy.Node].Shards[shard].Read(call, Query{Limit: 1})

			cancel()

			if err == nil {
				donor = true
			}
		}
	}

	if !donor {
		return errors.New("replacement needs a reachable serving donor")
	}

	found := false

	for i, copy := range t.Shards[shard] {
		if copy.Node == newNode {
			return errors.New("node already holds shard")
		}

		if copy.Node == old {
			t.Shards[shard][i] = placement.Copy{Node: newNode, State: "behind"}
			found = true
		}
	}

	if !found {
		return errors.New("old holder missing")
	}

	present := false

	for _, n := range t.Nodes {
		if n == newNode {
			present = true
		}
	}

	if !present {
		t.Nodes = append(t.Nodes, newNode)
	}

	t.Generation++
	if err = c.Placement.CompareAndSwap(ctx, v, t); err != nil {
		return err
	}

	if err = c.Refill(ctx, shard, newNode); err != nil {
		return err
	}

	if previous := c.Nodes[old]; previous != nil {
		call, cancel := c.timeout(ctx)
		defer cancel()

		tx, err := previous.Shards[shard].begin(call, false)
		if err == nil {
			_, err = tx.Exec(call, `TRUNCATE hazard_events,stream_state,unprocessed_messages; UPDATE source_progress SET checkpoint=NULL,last_success_at=NULL,stale_since=NULL`)
			if err == nil {
				_ = tx.Commit(call)
			}

			_ = tx.Rollback(call)
		}
	}

	return nil
}

func (s *Store) restoreParked(ctx context.Context, m ParkedMessage) error {
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `INSERT INTO unprocessed_messages (`+parkingColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
 ON CONFLICT(message_id) DO UPDATE SET status=CASE WHEN unprocessed_messages.status='recovered' THEN 'recovered' ELSE EXCLUDED.status END,
 attempts=GREATEST(unprocessed_messages.attempts,EXCLUDED.attempts),next_attempt_at=EXCLUDED.next_attempt_at`, m.MessageID, m.Source, m.ReceivedAt, m.SourceRefID, m.Raw, m.RawLength, m.RawSHA256, m.RawCut, m.ReasonCode, m.Reason, m.Retryable, m.Attempts, m.NextAttemptAt, m.Status, m.CorrelationID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
