//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
)

type memoryPlacement struct {
	mu      sync.Mutex
	table   placement.Table
	version int
}

func (m *memoryPlacement) Load(context.Context) (placement.Table, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	raw, _ := json.Marshal(m.table)

	var clone placement.Table

	_ = json.Unmarshal(raw, &clone)

	return clone, fmt.Sprint(m.version), nil
}

func (m *memoryPlacement) CompareAndSwap(_ context.Context, v string, t placement.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if v != fmt.Sprint(m.version) {
		return placement.ErrConflict
	}

	m.table = t
	m.version++

	return nil
}

func dataCluster(t *testing.T) *Cluster {
	t.Helper()

	ctx := context.Background()
	nodes := map[string]*DataNode{}

	for i, name := range []string{"a", "b", "c"} {
		url := os.Getenv([]string{"DATA_TEST_A", "DATA_TEST_B", "DATA_TEST_C"}[i])
		if url == "" {
			t.Fatal("run make data-integration")
		}

		node, err := OpenDataNode(ctx, name, url, DefaultClusterLimits())
		if err != nil {
			t.Fatal(err)
		}

		nodes[name] = node
		if _, err = node.Store.ingest.Exec(ctx, `TRUNCATE public.publication_outbox,public.unprocessed_messages,public.unprocessed_payload_chunks; UPDATE public.source_fence SET epoch=1; UPDATE public.ingestion_clock SET last_value='epoch'`); err != nil {
			t.Fatal(err)
		}

		t.Cleanup(node.Store.Close)

		for _, s := range node.Shards {
			tx, err := s.begin(ctx, false)
			if err != nil {
				t.Fatal(err)
			}

			_, err = tx.Exec(ctx, `TRUNCATE hazard_events,stream_state,unprocessed_messages,unprocessed_payload_chunks,field_reports_inbox; UPDATE source_progress SET checkpoint=NULL,last_success_at=NULL,stale_since=NULL`)
			if err == nil {
				err = tx.Commit(ctx)
			}

			_ = tx.Rollback(ctx)

			if err != nil {
				t.Fatal(err)
			}
		}
	}

	table, _ := placement.New([]string{"a", "b", "c"})
	repo := &memoryPlacement{table: table}

	limits := DefaultClusterLimits()
	limits.ShardDeadline = time.Second

	c := &Cluster{Nodes: nodes, Placement: repo, Limits: limits}
	for _, source := range []string{"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"} {
		if err := c.Seal(ctx, source, 1); err != nil {
			t.Fatal(err)
		}
	}

	for name := range c.Nodes {
		if _, err := c.detectStore(ctx, name); err != nil {
			t.Fatal(err)
		}
	}

	return c
}

func TestDataIntegrationAuditUnprocessedPartial(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()

	now := time.Now().UTC()
	for i, record := range []struct{ id, source string }{
		{"bmkg", SourceBMKG}, {"pvmbg-one", SourcePVMBG}, {"pvmbg-two", SourcePVMBG},
	} {
		message := ParkedMessage{MessageID: record.id, Source: record.source, ReceivedAt: now.Add(time.Duration(i) * time.Second), Raw: []byte("bad"), ReasonCode: "schema_incompatible", Reason: "unknown schema"}
		if err := c.Park(ctx, message); err != nil {
			t.Fatal(err)
		}
	}

	handler := (&AuditAPI{Store: c, Parking: c, Token: "audit"}).Handler()
	request := func(query string, status int) ParkingPage {
		t.Helper()

		r := httptest.NewRequest("GET", "/internal/audit/unprocessed"+query, nil)
		r.Header.Set("Authorization", "Bearer audit")

		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)

		if w.Code != status {
			t.Fatalf("status %d, want %d: %s", w.Code, status, w.Body)
		}

		var page ParkingPage
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}

		return page
	}
	failReads := func(store *Store, table string) {
		t.Helper()
		execData(t, store, "REVOKE SELECT ON "+table+" FROM aggregator")
		t.Cleanup(func() { execData(t, store, "GRANT SELECT ON "+table+" TO aggregator") })
	}

	shard := placement.Shard("metadata:" + SourceBMKG)

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	copies := table.Shards[shard]
	first := c.Nodes[copies[0].Node].Shards[shard]
	failReads(first, first.namespace+".unprocessed_messages")

	if page := request("", 200); page.Partial || len(page.Items) != 3 {
		t.Fatalf("readable replica should return a complete page: %+v", page)
	}

	second := c.Nodes[copies[1].Node].Shards[shard]
	failReads(second, second.namespace+".unprocessed_messages")
	failReads(c.Nodes["c"].Store, "public.unprocessed_messages")

	page := request("?limit=1", 200)
	wantUnits := []string{shard, "c/public"}
	slices.Sort(wantUnits)

	if !page.Partial || !slices.Equal(page.UnavailableUnits, wantUnits) || page.NextCursor == nil || len(page.Items) != 1 || page.Items[0].MessageID != "pvmbg-one" {
		t.Fatalf("wrong partial response: %+v", page)
	}

	page = request("?limit=1&cursor="+url.QueryEscape(*page.NextCursor), 200)
	if !page.Partial || len(page.Items) != 1 || page.Items[0].MessageID != "pvmbg-two" {
		t.Fatalf("wrong next partial page: %+v", page)
	}

	request("?cursor=invalid", 400)

	for _, name := range []string{"a", "b"} {
		failReads(c.Nodes[name].Store, "public.unprocessed_messages")
	}

	request("?source=BMKG", 503)
}

func TestDataIntegrationDualWriteWipeRefillAndNextLoss(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)

	batch := WriteBatch{Source: "BMKG", Epoch: 1, Now: now}
	for i := 0; i < 150; i++ {
		event := contractEvent("BMKG", fmt.Sprintf("quake-%d", i), now.Add(time.Duration(i)*time.Second))
		batch.Events = append(batch.Events, event)
	}

	if _, err := c.Upsert(ctx, batch); err != nil {
		t.Fatal(err)
	}

	page, err := c.Read(ctx, Query{Limit: 500})
	if err != nil || len(page.Data) != 150 || page.Partial {
		t.Fatal("initial read", len(page.Data), err)
	}

	metrics, err := c.NodeMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pending := int64(0)

	for _, node := range c.Nodes {
		depth, oldest, err := node.Store.OutboxStats(ctx)
		if err != nil || depth > 0 && oldest <= 0 {
			t.Fatal("invalid pending outbox snapshot", depth, oldest, err)
		}

		pending += depth
	}

	if pending == 0 {
		t.Fatal("new events did not appear in outbox measurements")
	}

	physical := int64(0)

	for _, m := range metrics {
		if m.Unavailable || m.Bytes <= 0 {
			t.Fatal("invalid node metrics", m)
		}

		physical += m.Records
	}

	if physical != 300 {
		t.Fatal("physical replica count", physical)
	}

	for _, node := range c.Nodes {
		count := 0

		for _, s := range node.Shards {
			r, err := s.Read(ctx, Query{Limit: 500})
			if err != nil {
				t.Fatal(err)
			}

			count += len(r.Data)
		}

		if count == 0 || count >= 150 {
			t.Fatal("node has empty/all data", node.Name, count)
		}

		t.Logf("node=%s records=%d", node.Name, count)
	}

	table, _, _ := c.Placement.Load(ctx)
	for shard, copies := range table.Shards {
		for _, copy := range copies {
			if copy.Node == "b" {
				if err := c.MarkBehind(ctx, BehindRequest{Source: "BMKG", Epoch: 1, Shard: shard, Copy: "b", StoreID: copy.StoreID}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	// Simulate a wiped volume: drop every shard and recreate it empty.
	b := c.Nodes["b"]
	for shard, s := range b.Shards {
		if _, err = b.Store.ingest.Exec(ctx, `DROP SCHEMA `+s.namespace+` CASCADE`); err != nil {
			t.Fatal(err)
		}

		b.Shards[shard], err = b.Store.initShard(ctx, s.namespace)
		if err != nil {
			t.Fatal(err)
		}
	}

	page, err = c.Read(ctx, Query{Limit: 500})
	if err != nil || len(page.Data) != 150 || page.Partial {
		t.Fatal("wipe lost data", len(page.Data), err)
	}
	// Accept new events during recovery, then restore every wiped copy.
	batch.Events = []HazardEvent{contractEvent("BMKG", "during-refill", now)}
	if _, err = c.Upsert(ctx, batch); err != nil {
		t.Fatal(err)
	}

	observed := 0
	c.OnRefill = func(_ context.Context, shard, node string, elapsed time.Duration, err error) {
		if shard == "" || node != "b" || elapsed <= 0 || err != nil {
			t.Errorf("invalid refill telemetry: %s %s %s %v", shard, node, elapsed, err)
		}

		observed++
	}

	expected := 0

	for shard, copies := range table.Shards {
		for _, copy := range copies {
			if copy.Node == "b" {
				expected++

				if err = c.Refill(ctx, shard, "b"); err != nil {
					t.Fatal("refill", shard, err)
				}
			}
		}
	}

	if observed == 0 || observed != expected {
		t.Fatalf("observed %d refills, expected %d", observed, expected)
	}

	a := c.Nodes["a"]

	delete(c.Nodes, "a")
	defer func() { c.Nodes["a"] = a }()

	page, err = c.Read(ctx, Query{Limit: 500})
	if err != nil || len(page.Data) != 151 || page.Partial {
		t.Fatal("next loss", len(page.Data), page.Partial, err)
	}

	delete(c.Nodes, "c")

	page, err = c.Read(ctx, Query{Limit: 500})
	if err != nil || !page.Partial || len(page.UnavailableUnits) == 0 {
		t.Fatal("missing partial markers", page, err)
	}

	t.Log("verified dual placement, empty-copy refill, continued writes, second loss, and explicit partial reads")
}

func TestDataIntegrationReplicatedContract(t *testing.T) {
	runShardStoreContract(t, func(t *testing.T) ShardStore { return dataCluster(t) })
}

func TestDataIntegrationLiveRefillAndAudit(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	event := contractEvent("BMKG", "live", now)
	shard := placement.Shard(event.HazardID)

	initial := WriteBatch{Source: "BMKG", Epoch: 1, Events: []HazardEvent{event}, Now: now}
	if _, err := c.Upsert(ctx, initial); err != nil {
		t.Fatal(err)
	}

	table, _, _ := c.Placement.Load(ctx)

	target := table.Shards[shard][1].Node
	if err := c.MarkBehind(ctx, BehindRequest{Source: "BMKG", Epoch: 1, Shard: shard, Copy: target, StoreID: table.Shards[shard][1].StoreID}); err != nil {
		t.Fatal(err)
	}

	dest := c.Nodes[target].Shards[shard]

	tx, err := dest.begin(ctx, false)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(ctx, `TRUNCATE hazard_events`)
	if err == nil {
		err = tx.Commit(ctx)
	}

	_ = tx.Rollback(ctx)

	if err != nil {
		t.Fatal(err)
	}

	writes := make(chan error, 1)

	go func() {
		for i := 0; i < 30; i++ {
			b := initial

			b.Events = []HazardEvent{contractEvent("BMKG", fmt.Sprintf("live-%d", i), now)}
			if _, err := c.Upsert(ctx, b); err != nil {
				writes <- err
				return
			}
		}

		writes <- nil
	}()

	refillErr := c.Refill(ctx, shard, target)

	if err := <-writes; err != nil {
		t.Fatal("live writes", err)
	}

	if refillErr != nil {
		if err := c.Refill(ctx, shard, target); err != nil {
			t.Fatal("refill retry", err)
		}
	}
	// Finish all needed repair and compare full canonical rows on both holders.
	a, err := shardDigest(ctx, c.Nodes[table.Shards[shard][0].Node].Shards[shard])
	if err != nil {
		t.Fatal(err)
	}

	b, err := shardDigest(ctx, dest)
	if err != nil || a != b {
		t.Fatal("refill diverged", a, b, err)
	}

	all, err := c.Checksum(ctx, "BMKG")
	if err != nil || all.Count != 31 || all.Partial {
		t.Fatal("audit count", all, err)
	}

	summary, err := Analytics(ctx, c, nil, nil)
	if err != nil || summary.Total != 31 {
		t.Fatal("analytics vs audit", summary.Total, err)
	}

	q := Query{Limit: 7, Audit: true}
	seen := map[string]bool{}

	for {
		page, err := c.Read(ctx, q)
		if err != nil {
			t.Fatal(err)
		}

		for _, h := range page.Data {
			if seen[h.HazardID] {
				t.Fatal("duplicate scatter page")
			}

			seen[h.HazardID] = true
		}

		if page.NextCursor == nil {
			break
		}

		q.Cursor = *page.NextCursor
	}

	if len(seen) != 31 {
		t.Fatal("scatter pages lost rows", len(seen))
	}

	t.Log("live writes and replica refill agree; Audit, analytics and paginated scatter reads agree")
}

func TestDataIntegrationAutomaticWipeRecoveryAndParking(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	event := contractEvent("PVMBG", "recovered", now)
	if _, err := c.Upsert(ctx, WriteBatch{Source: "PVMBG", Epoch: 1, Now: now, Events: []HazardEvent{event}}); err != nil {
		t.Fatal(err)
	}

	m := ParkedMessage{MessageID: "replicated-message", Source: "PVMBG", SourceRefID: "new", ReceivedAt: now, Raw: []byte(`{"new":1}`), ReasonCode: "schema_incompatible", Reason: "changed schema"}
	if err := c.Park(ctx, m); err != nil {
		t.Fatal(err)
	}
	// A node returns with empty shard schemas, without manually marking placement.
	node := c.Nodes["b"]
	for _, shard := range node.Shards {
		if _, err := node.Store.ingest.Exec(ctx, `DROP SCHEMA `+shard.namespace+` CASCADE`); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.RecoverOnce(ctx); err != nil {
		t.Fatal("automatic refill", err)
	}

	checksum, err := c.Checksum(ctx, "PVMBG")
	if err != nil || checksum.Count != 1 || checksum.Partial {
		t.Fatal("recovery lost events", checksum, err)
	}

	page, err := c.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 1 {
		t.Fatal("parking lost on recovery", page, err)
	}

	count, err := c.Replay(ctx, "PVMBG", 1, 100, func(m ParkedMessage) (HazardEvent, error) { return contractEvent(m.Source, m.SourceRefID, now), nil })
	if err != nil || count != 1 {
		t.Fatal("replicated replay", count, err)
	}

	page, err = c.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("recovery flag", page, err)
	}
}

func TestDataIntegrationOversizedPayloadSurvivesRefill(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	source := SourcePVMBG

	good, err := json.Marshal(fixture(t, "volcanic-v1.json")[0])
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`[` + string(good) + `,{"oversized":"` + strings.Repeat("0123456789abcdef", (9<<20)/16) + `"}]`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer server.Close()

	poller := &Poller{Source: source, BaseURL: server.URL, Client: server.Client(), Timeout: defaultSourceTimeout, Log: testLog(), Store: &EpochIngest{Data: c, Source: source, WriterEpoch: 1}}
	sha := fmt.Sprintf("%x", sha256.Sum256(body))
	path := "/volcanic-reports"
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(source+"\x00"+path+"\x00"+sha)))

	legacy := ParkedMessage{MessageID: id, Source: source, ReceivedAt: time.Now().UTC(), Raw: body[:64<<10], RawSHA256: sha, RawLength: int64(len(body)), RawCut: true, ReasonCode: "body_too_large", Reason: "legacy prefix"}
	if err := c.Park(ctx, legacy); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if err := poller.Poll(ctx, "oversized-test"); err != nil {
			t.Fatal("full parking failed", err)
		}
	}

	hazards, err := c.Read(ctx, Query{Source: source, Limit: 10})
	if err != nil || len(hazards.Data) != 1 || hazards.Data[0].Revision != 1 {
		t.Fatal("valid record lost or duplicated", hazards.Data, err)
	}

	page, err := c.ListUnprocessed(ctx, source, "", 10)
	if err != nil || len(page.Items) != 1 || page.Items[0].MessageID != id || !page.Items[0].RawChunked || page.Items[0].RawCut || page.Items[0].RawSHA256 != sha || page.Items[0].RawLength != int64(len(body)) {
		t.Fatal("wrong parked metadata or duplicate", page, err)
	}

	shard := placement.Shard("metadata:" + source)

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	verify := func(store *Store) {
		t.Helper()

		offset := 0

		for part := int64(0); ; part++ {
			result, err := store.ReadParkingPart(ctx, source, id, part)

			end := min(offset+parkingChunkBytes, len(body))
			if err != nil || !bytes.Equal(result.Data, body[offset:end]) {
				t.Fatalf("part %d differs from ingested bytes: %v", part, err)
			}

			if end == len(body) {
				if result.NextPart != nil {
					t.Fatal("last part must have a null next_part")
				}

				break
			}

			if result.NextPart == nil || *result.NextPart != part+1 {
				t.Fatal("missing next part", part)
			}

			offset = end
		}
	}

	copies := table.Shards[shard]
	for _, copy := range copies {
		verify(c.Nodes[copy.Node].Shards[shard])
	}

	target := copies[1]
	store := c.Nodes[target.Node].Shards[shard]
	execData(t, store, `TRUNCATE `+store.namespace+`.unprocessed_messages,`+store.namespace+`.unprocessed_payload_chunks`)

	if err = c.MarkBehind(ctx, BehindRequest{Source: source, Epoch: 1, Shard: shard, Copy: target.Node, StoreID: target.StoreID}); err != nil {
		t.Fatal(err)
	}

	if err = c.Refill(ctx, shard, target.Node); err != nil {
		t.Fatal(err)
	}

	verify(store)

	ops := (&OpsAPI{Parking: c, Token: "ops", Capacity: 1}).Handler()
	last := (len(body) - 1) / parkingChunkBytes
	query := url.Values{"source": {source}, "message_id": {id}, "part": {strconv.Itoa(last)}}
	r := httptest.NewRequest(http.MethodGet, "/internal/ops/unprocessed?"+query.Encode(), nil)
	w := httptest.NewRecorder()
	ops.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatal("payload must require Ops authorization", w.Code)
	}

	r.Header.Set("Authorization", "Bearer ops")

	w = httptest.NewRecorder()
	ops.ServeHTTP(w, r)

	var part ParkingPart
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &part) != nil || !bytes.Equal(part.Data, body[last*parkingChunkBytes:]) || part.NextPart != nil {
		t.Fatal("Ops did not return the final payload part", w.Code)
	}
}

func TestDataIntegrationNewNodeJoinsWithoutStoppingReads(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	events := []HazardEvent{}
	for i := 0; i < 100; i++ {
		events = append(events, contractEvent("BMKG", fmt.Sprintf("join-%d", i), now))
	}

	if _, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: events}); err != nil {
		t.Fatal(err)
	}

	before, err := c.Checksum(ctx, "BMKG")
	if err != nil {
		t.Fatal(err)
	}

	url := os.Getenv("DATA_TEST_D")
	if url == "" {
		t.Fatal("fourth local database required")
	}

	node, err := OpenDataNode(ctx, "d", url, DefaultClusterLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer node.Store.Close()

	c.Nodes["d"] = node
	for _, s := range node.Shards {
		tx, err := s.begin(ctx, false)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.Exec(ctx, `TRUNCATE hazard_events,stream_state,unprocessed_messages,unprocessed_payload_chunks`)
		if err == nil {
			err = tx.Commit(ctx)
		}

		_ = tx.Rollback(ctx)

		if err != nil {
			t.Fatal(err)
		}
	}

	if err = c.RebalanceOnce(ctx); err != nil {
		t.Fatal(err)
	}

	after, err := c.Checksum(ctx, "BMKG")
	if err != nil || after.Count != before.Count || after.Checksum != before.Checksum || after.Partial {
		t.Fatal("join changed canonical data", before, after, err)
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	held := 0

	for _, copies := range table.Shards {
		for _, copy := range copies {
			if copy.Node == "d" {
				held++
			}
		}
	}

	if held == 0 {
		t.Fatal("new node has no shards")
	}

	if _, err = c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: []HazardEvent{contractEvent("BMKG", "post-join", now)}}); err != nil {
		t.Fatal(err)
	}

	t.Logf("new node holds %d copies; checksum unchanged across join; writes continue", held)
}

func TestDataIntegrationTakeoverExcludesUnsealedReplica(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	offline := c.Nodes["c"]
	delete(c.Nodes, "c")

	if err := c.Seal(ctx, "BMKG", 2); err != nil {
		t.Fatal(err)
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, copies := range table.Shards {
		for _, copy := range copies {
			if copy.Node == "c" && copy.State != "behind" {
				t.Fatal("unsealed replica still serves reads")
			}
		}
	}

	now := time.Now().UTC()
	if _, err = c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now}); !errors.Is(err, ErrStaleEpoch) {
		t.Fatal("old writer survived takeover", err)
	}

	c.Nodes["c"] = offline
	if err = c.RecoverOnce(ctx); err != nil {
		t.Fatal("takeover recovery", err)
	}

	epoch, err := offline.Store.Epoch(ctx, "BMKG")
	if err != nil || epoch != 2 {
		t.Fatal("returned replica not sealed", epoch, err)
	}
}

func TestDataIntegrationConcurrentWritersDoNotStarveIngestionPool(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC()
	errs := make(chan error, 8)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			_, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: []HazardEvent{contractEvent("BMKG", fmt.Sprintf("concurrent-%d", i), now)}})
			errs <- err
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, copies := range table.Shards {
		for _, copy := range copies {
			if copy.State != "serving" {
				t.Fatal("healthy node marked behind by pool starvation", copy)
			}
		}
	}

	checksum, err := c.Checksum(ctx, "BMKG")
	if err != nil || checksum.Count != 8 {
		t.Fatal(checksum, err)
	}
}

func TestDataIntegrationReadPreservesFreshnessWhenShardQueriesExpire(t *testing.T) {
	cluster := dataCluster(t)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	now := time.Now().UTC()

	batch := WriteBatch{
		Source: SourceBMKG, Epoch: 1, Now: now,
		Events: []HazardEvent{contractEvent(SourceBMKG, "read-deadline", now)},
	}
	if _, err := cluster.CommitCheckpoint(ctx, batch, now); err != nil {
		t.Fatal(err)
	}

	for _, node := range cluster.Nodes {
		tx, err := node.Store.ingest.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())

		tables := make([]string, 0, len(node.Shards))
		for _, shard := range node.Shards {
			tables = append(tables, pgx.Identifier{shard.namespace, "hazard_events"}.Sanitize())
		}

		if _, err = tx.Exec(ctx, `LOCK TABLE `+strings.Join(tables, ",")+` IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
	}

	api := &API{
		Store: cluster, Token: "test-internal", Timeout: 600 * time.Millisecond,
		Admission: NewAdmission(4, 0), Log: testLog(),
	}

	response := request(api.Handler(), "/internal/hazards?source=BMKG&limit=20", "test-internal")
	if response.Code != http.StatusOK {
		t.Fatalf("got %d, want 200 with partial data: %s", response.Code, response.Body)
	}

	var got Response
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}

	if !got.Partial || len(got.UnavailableUnits) != placement.Shards ||
		len(got.Sources) != 1 || got.Sources[0].Status != "healthy" {
		t.Fatalf("missing partial marker or healthy source: %+v", got)
	}
}
