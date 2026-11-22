package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
)

// Refill must be called by the holder of the placement Lease. It first enables
// dual writes to the target without serving reads from it. Revision-conditional
// copy cannot overwrite a newer live write. A failed live write changes the
// placement version, so the final CAS cannot promote that failed copy.
func (c *Cluster) Refill(ctx context.Context, shard, target string) (err error) {
	if c.OnRefill != nil {
		started := time.Now()
		defer func() { c.OnRefill(ctx, shard, target, time.Since(started), err) }()
	}

	var donor placement.Copy

	err = placement.Update(ctx, c.Placement, func(t *placement.Table) (bool, error) {
		found := false
		donor = placement.Copy{}

		for _, holder := range t.Shards[shard] {
			if holder.Node == target {
				found = true
			} else if holder.State == placement.Serving {
				donor = holder
			}
		}

		if !found || donor.Node == "" || c.Nodes[target] == nil || c.Nodes[donor.Node] == nil {
			return false, errors.New("refill needs a serving source and configured target")
		}

		return true, t.Transition(shard, target, placement.Refilling)
	})
	if err != nil {
		return err
	}

	t, v, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	var targetCopy placement.Copy

	for _, holder := range t.Shards[shard] {
		if holder.Node == target {
			targetCopy = holder
		}
	}

	source, dest := c.copyStore(donor, shard), c.copyStore(targetCopy, shard)
	if err = copyShard(ctx, source, dest); err != nil {
		return err
	}
	// Concurrent dual writes can make two snapshots differ transiently. Retry,
	// leaving the target non-serving if verification cannot establish equality.
	verified := false

	for range c.Limits.RefillAttempts {
		a, err := shardDigest(ctx, source)
		if err != nil {
			return err
		}

		b, err := shardDigest(ctx, dest)
		if err != nil {
			return err
		}

		if equalRefillDigests(a, b) {
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

	if err = t.Transition(shard, target, placement.Serving); err != nil {
		return err
	}

	return c.swapPlacement(ctx, v, t)
}

// copyShard copies every table of a shard from one snapshot, so the events, state, and
// checkpoints that reach the target agree with each other.
func copyShard(ctx context.Context, from, to *Store) error {
	tx, err := from.begin(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, step := range []func(context.Context, pgx.Tx, *Store) error{
		copyEvents, copyPendingPublications, copyParkedPublications, copyStreamState, copyParking, copyFieldReports, copyProgress,
	} {
		if err = step(ctx, tx, to); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func copyEvents(ctx context.Context, tx pgx.Tx, to *Store) error {
	after := ""

	for {
		batch, err := queryPage(ctx, tx, `SELECT `+columns+` FROM hazard_events WHERE hazard_id COLLATE "C" > $1 ORDER BY hazard_id COLLATE "C" LIMIT 100`, after, scanEvent)
		if err != nil || len(batch) == 0 {
			return err
		}

		if err = to.restoreEvents(ctx, batch); err != nil {
			return err
		}

		after = batch[len(batch)-1].HazardID
	}
}

func copyParking(ctx context.Context, tx pgx.Tx, to *Store) error {
	after := ""

	for {
		batch, err := queryPage(ctx, tx, `SELECT `+parkingColumns+` FROM unprocessed_messages WHERE message_id>$1 ORDER BY message_id LIMIT 100`, after, scanParked)
		if err != nil || len(batch) == 0 {
			return err
		}

		for _, m := range batch {
			if err = to.restoreParked(ctx, m); err != nil {
				return err
			}
		}

		after = batch[len(batch)-1].MessageID
	}
}

// queryPage reads one keyset page and closes its rows before the caller writes elsewhere.
func queryPage[T any](ctx context.Context, tx pgx.Tx, sql, after string, scan func(pgx.Row) (T, error)) ([]T, error) {
	rows, err := tx.Query(ctx, sql, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	page := []T{}

	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, err
		}

		page = append(page, item)
	}

	return page, rows.Err()
}

// copyStreamState uses the snapshot of the copied events. The target keeps the greater
// offset if a live commit raced the refill.
func copyStreamState(ctx context.Context, tx pgx.Tx, to *Store) error {
	rows, err := tx.Query(ctx, `SELECT source,committed_offset,payload FROM stream_state`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			source string
			offset int64
			raw    []byte
		)

		if err = rows.Scan(&source, &offset, &raw); err != nil {
			return err
		}

		if err = to.restoreState(ctx, source, offset, raw); err != nil {
			return err
		}
	}

	return rows.Err()
}

func copyFieldReports(ctx context.Context, tx pgx.Tx, to *Store) error {
	rows, err := tx.Query(ctx, `SELECT report_id,payload,received_at FROM field_reports_inbox ORDER BY report_id`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id       string
			payload  []byte
			received time.Time
		)

		if err = rows.Scan(&id, &payload, &received); err != nil {
			return err
		}

		if err = to.restoreFieldReport(ctx, id, payload, received); err != nil {
			return err
		}
	}

	return rows.Err()
}

// copyProgress keeps the later checkpoint and success time on the target.
func copyProgress(ctx context.Context, tx pgx.Tx, to *Store) error {
	for _, source := range Sources {
		var (
			progress   Freshness
			checkpoint *time.Time
		)

		err := tx.QueryRow(ctx, `SELECT checkpoint,last_success_at,stale_since FROM source_progress WHERE source=$1`, source).Scan(&checkpoint, &progress.LastSuccessAt, &progress.StaleSince)
		if err != nil {
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

	return nil
}

func equalRefillDigests(source, target string) bool { return source == target }

func (s *Store) restoreEvents(ctx context.Context, events []HazardEvent) error {
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, h := range events {
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
	}

	return tx.Commit(ctx)
}

// Copy only outstanding publications of this shard from the donor snapshot.
// Rebuilding canonical rows must not enqueue already delivered history.
func copyPendingPublications(ctx context.Context, snapshot pgx.Tx, to *Store) error {
	afterID := ""

	var afterRevision int64
	for {
		rows, err := snapshot.Query(ctx, `SELECT o.hazard_id,o.revision,o.payload,o.correlation_id,o.critical,o.created_at
 FROM public.publication_outbox o JOIN hazard_events h ON h.hazard_id=o.hazard_id
 WHERE (o.hazard_id COLLATE "C",o.revision)>($1,$2) ORDER BY o.hazard_id COLLATE "C",o.revision LIMIT 100`, afterID, afterRevision)
		if err != nil {
			return err
		}

		type pending struct {
			id          string
			revision    int64
			raw         []byte
			correlation string
			critical    bool
			created     time.Time
		}

		batch := []pending{}

		for rows.Next() {
			var row pending
			if err = rows.Scan(&row.id, &row.revision, &row.raw, &row.correlation, &row.critical, &row.created); err != nil {
				rows.Close()
				return err
			}

			batch = append(batch, row)
		}

		err = rows.Err()
		rows.Close()

		if err != nil {
			return err
		}

		if len(batch) == 0 {
			return nil
		}

		target, err := to.begin(ctx, false)
		if err != nil {
			return err
		}

		for _, row := range batch {
			_, err = target.Exec(ctx, `INSERT INTO public.publication_outbox(hazard_id,revision,payload,correlation_id,critical,created_at) VALUES($1,$2,$3::jsonb,$4,$5,$6) ON CONFLICT DO NOTHING`, row.id, row.revision, string(row.raw), row.correlation, row.critical, row.created)
			if err != nil {
				break
			}
		}

		if err == nil {
			err = target.Commit(ctx)
		}

		_ = target.Rollback(ctx)

		if err != nil {
			return err
		}

		afterID, afterRevision = batch[len(batch)-1].id, batch[len(batch)-1].revision
	}
}

// Failed publications must survive the same second-node loss as the outbox.
func copyParkedPublications(ctx context.Context, snapshot pgx.Tx, to *Store) error {
	root := *to
	root.namespace = ""
	after := ""

	qualified := "p." + strings.ReplaceAll(parkingColumns, ",", ",p.")
	for {
		rows, err := snapshot.Query(ctx, `SELECT `+qualified+` FROM public.unprocessed_messages p
 JOIN hazard_events h ON p.message_id LIKE 'publication:' || h.hazard_id || ':%'
 WHERE p.reason_code='publish_failed' AND p.status<>'recovered' AND p.message_id>$1
 ORDER BY p.message_id LIMIT 100`, after)
		if err != nil {
			return err
		}

		batch := []ParkedMessage{}

		for rows.Next() {
			m, err := scanParked(rows)
			if err != nil {
				rows.Close()
				return err
			}

			batch = append(batch, m)
		}

		err = rows.Err()
		rows.Close()

		if err != nil {
			return err
		}

		if len(batch) == 0 {
			return nil
		}

		for _, m := range batch {
			// A donor can have a requeued publication awaiting confirmation. Copy its
			// retry as due too, so a lost outbox row cannot leave a permanent NULL retry.
			if m.Status == "retrying" && m.NextAttemptAt == nil {
				now := time.Now().UTC()
				m.NextAttemptAt = &now
			}

			if err = root.restoreParked(ctx, m); err != nil {
				return err
			}
		}

		after = batch[len(batch)-1].MessageID
	}
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

	hash := sha256.New()

	after := ""
	for {
		rows, err := tx.Query(ctx, `SELECT hazard_id,row_to_json(h)::text FROM
 (SELECT `+columns+` FROM hazard_events WHERE hazard_id COLLATE "C">$1 ORDER BY hazard_id COLLATE "C" LIMIT 1000) h`, after)
		if err != nil {
			return "", err
		}

		count := 0

		for rows.Next() {
			var raw string
			if err = rows.Scan(&after, &raw); err != nil {
				rows.Close()
				return "", err
			}

			_, _ = fmt.Fprintln(hash, raw)
			count++
		}

		err = rows.Err()
		rows.Close()

		if err != nil {
			return "", err
		}

		if count < 1000 {
			break
		}
	}

	reports, err := tx.Query(ctx, `SELECT row_to_json(r)::text FROM (SELECT report_id,payload FROM field_reports_inbox ORDER BY report_id COLLATE "C") r`)
	if err != nil {
		return "", err
	}

	for reports.Next() {
		var raw string
		if err = reports.Scan(&raw); err != nil {
			reports.Close()
			return "", err
		}

		_, _ = fmt.Fprintln(hash, "field_report", raw)
	}

	err = reports.Err()
	reports.Close()

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

	storeID, err := c.prepareNode(ctx, t, newNode)
	if err != nil {
		return err
	}

	if !c.reachableDonor(ctx, t, shard, old) {
		return errors.New("replacement needs a reachable serving donor")
	}

	if err = replaceHolder(&t, shard, old, newNode, storeID); err != nil {
		return err
	}

	if err = c.swapPlacement(ctx, v, t); err != nil {
		return err
	}

	// Retain the retired copy outside placement. A paused former planner must
	// never truncate a schema that a newer planner may already have reassigned.
	// Physical cleanup requires its own fenced ownership protocol.
	return c.Refill(ctx, shard, newNode)
}

// prepareNode seals every writer epoch on the new node before it can hold a copy, and returns its store identity.
func (c *Cluster) prepareNode(ctx context.Context, t placement.Table, node string) (string, error) {
	if c.Nodes[node] == nil {
		return "", errors.New("new node is not configured")
	}

	for source, epoch := range t.WriterEpochs {
		call, cancel := c.timeout(ctx)
		err := c.Nodes[node].Store.Seal(call, source, epoch)

		cancel()

		if err != nil {
			return "", err
		}
	}

	call, cancel := c.timeout(ctx)
	defer cancel()

	return c.Nodes[node].Store.identity(call)
}

func (c *Cluster) reachableDonor(ctx context.Context, t placement.Table, shard, old string) bool {
	others := slices.DeleteFunc(slices.Clone(t.Shards[shard]), func(holder placement.Copy) bool { return holder.Node == old })

	_, err := readCopies(ctx, c, others, shard, 0, func(call context.Context, s *Store) (Response, error) {
		return s.Read(call, Query{Limit: 1})
	})

	return err == nil
}

// replaceHolder puts node in place of old as a copy that is behind until its refill.
func replaceHolder(t *placement.Table, shard, old, node, storeID string) error {
	found := false

	for i, holder := range t.Shards[shard] {
		if holder.Node == node {
			return errors.New("node already holds shard")
		}

		if holder.Node == old {
			t.Shards[shard][i] = placement.Copy{Node: node, State: placement.Behind, StoreID: storeID}
			found = true
		}
	}

	if !found {
		return errors.New("old holder missing")
	}

	if !slices.Contains(t.Nodes, node) {
		t.Nodes = append(t.Nodes, node)
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
