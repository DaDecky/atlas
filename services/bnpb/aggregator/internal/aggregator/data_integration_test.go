package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIntegrationDataAuditPaginationAnalyticsOutboxParking(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	batch := WriteBatch{Source: "BMKG", Epoch: 1, Now: now}

	refs := []string{"z", "é", "A", "a", "中"}
	for _, ref := range refs {
		h := contractEvent("BMKG", ref, now)
		batch.Events = append(batch.Events, h)
	}

	batch.Events[0].Attributes["potential_tsunami"] = json.RawMessage(`true`)
	if _, err := s.Upsert(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// Reference checksum is computed independently in byte order.
	sorted := append([]string(nil), refs...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))

	checksum, err := s.Checksum(ctx, "BMKG")
	if err != nil || checksum.Count != 5 || checksum.Checksum != hex.EncodeToString(sum[:]) {
		t.Fatal("checksum", checksum, err)
	}

	seen := map[string]bool{}

	q := Query{Source: "BMKG", Limit: 2, Audit: true}
	for {
		page, err := s.Read(ctx, q)
		if err != nil {
			t.Fatal(err)
		}

		for _, h := range page.Data {
			if seen[h.HazardID] {
				t.Fatal("duplicate page")
			}

			seen[h.HazardID] = true
		}

		if page.NextCursor == nil {
			break
		}

		q.Cursor = *page.NextCursor
	}

	if len(seen) != 5 {
		t.Fatal("pagination lost rows", seen)
	}

	q.Source = "PVMBG"
	if _, err = s.Read(ctx, q); err == nil {
		t.Fatal("cursor reused across filters")
	}

	summary, err := Analytics(ctx, s, nil, nil)
	if err != nil || summary.Total != 5 || summary.Types["TSUNAMI"] != 1 || summary.Types["SEISMIC"] != 4 {
		t.Fatal("analytics", summary, err)
	}

	audit := (&AuditAPI{Store: s, Parking: s, Token: "audit", Timeout: time.Second}).Handler()
	for path, status := range map[string]int{"/internal/audit/events?source=BMKG&limit=2": 200, "/internal/audit/events?source=BAD": 400, "/internal/audit/location?hazard_id=missing": 404, "/internal/audit/checksum?source=BMKG": 200} {
		if got := request(audit, path, "audit"); got.Code != status {
			t.Fatal(path, got.Code, got.Body.String())
		}
	}

	if got := request(audit, "/internal/audit/events", "internal"); got.Code != 401 {
		t.Fatal("audit auth", got.Code)
	}

	recorder := &relayRecorder{fail: true}
	if _, err = s.RelayOne(ctx, recorder); err == nil {
		t.Fatal("failed broker acknowledged")
	}

	recorder.fail = false

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for {
				sent, err := s.RelayOne(ctx, recorder)
				if err != nil {
					t.Error(err)
					return
				}

				if !sent {
					return
				}
			}
		})
	}

	wg.Wait()

	if len(recorder.sent) != 5 {
		t.Fatal("outbox loss/duplicate claims", recorder.sent)
	}

	for i := 0; i < 2; i++ {
		m := ParkedMessage{MessageID: fmt.Sprintf("msg-%d", i), Source: "PVMBG", SourceRefID: "same", ReceivedAt: now.Add(time.Duration(i) * time.Second), Raw: []byte{0xff, 0x00, 0x7b}, ReasonCode: "schema_incompatible", Reason: "field changed"}
		if err = s.Park(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	parked, err := s.ListUnprocessed(ctx, "PVMBG", "", 1)
	if err != nil || len(parked.Items) != 1 || string(parked.Items[0].Raw) != string([]byte{0xff, 0x00, 0x7b}) || parked.NextCursor == nil {
		t.Fatal("raw parking/pagination", parked, err)
	}

	second, err := s.ListUnprocessed(ctx, "PVMBG", *parked.NextCursor, 1)
	if err != nil || len(second.Items) != 1 || second.Items[0].MessageID == parked.Items[0].MessageID {
		t.Fatal("parking cursor", second, err)
	}

	decode := func(m ParkedMessage) (HazardEvent, error) {
		h := contractEvent(m.Source, m.SourceRefID, m.ReceivedAt)
		if m.MessageID == "msg-1" {
			h.Severity = "AWAS"
		}

		return h, nil
	}
	counts := make(chan int, 2)

	for range 2 {
		wg.Go(func() {
			count, err := s.Replay(ctx, "PVMBG", 1, 100, decode)
			if err != nil {
				t.Error(err)
			}

			counts <- count
		})
	}

	wg.Wait()
	close(counts)

	total := 0
	for n := range counts {
		total += n
	}

	if total != 2 {
		t.Fatal("replay effects", total)
	}

	remaining, err := s.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(remaining.Items) != 0 {
		t.Fatal("recovered still pending", remaining, err)
	}

	events, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || len(events.Data) != 1 || events.Data[0].Revision != 2 || events.Data[0].Severity != "AWAS" {
		t.Fatal("replay order", events, err)
	}
	// Rendering is tested as HTTP output without starting a browser/server.
	api := (&API{Store: s, Token: "internal", Timeout: time.Second, Admission: NewAdmission(1, 0), Log: testLog()}).Handler()

	w := request(api, "/internal/analytics", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "<svg") {
		t.Fatal("dashboard unavailable")
	}

	r := httptest.NewRequest("GET", "/internal/analytics/summary", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, r)

	if w.Code != 401 {
		t.Fatal("analytics leaked data")
	}
}

func TestIntegrationParkingAutomaticRetryAndClockRollback(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, m := range []ParkedMessage{
		{MessageID: "transient", Source: "PVMBG", SourceRefID: "transient", ReceivedAt: now.Add(-time.Hour), Raw: []byte(`{}`), ReasonCode: "source_unavailable", Reason: "timeout", Retryable: true},
		{MessageID: "permanent", Source: "PVMBG", SourceRefID: "permanent", ReceivedAt: now.Add(-time.Hour), Raw: []byte(`{}`), ReasonCode: "schema_incompatible", Reason: "type changed"},
	} {
		if err := s.Park(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	decode := func(m ParkedMessage) (HazardEvent, error) { return contractEvent(m.Source, m.SourceRefID, now), nil }

	count, err := s.RetryDue(ctx, "PVMBG", 1, 100, decode)
	if err != nil || count != 1 {
		t.Fatal("automatic retry", count, err)
	}

	remaining, err := s.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(remaining.Items) != 1 || remaining.Items[0].MessageID != "permanent" {
		t.Fatal("permanent message retried", remaining, err)
	}

	first := contractEvent("BMKG", "before-clock-step", now)
	if _, err = s.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: []HazardEvent{first}}); err != nil {
		t.Fatal(err)
	}

	old := now.Add(-time.Hour)

	second := contractEvent("BMKG", "after-clock-step", old)
	if _, err = s.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: old, Events: []HazardEvent{second}}); err != nil {
		t.Fatal(err)
	}

	page, err := s.Read(ctx, Query{Source: "BMKG", Audit: true, Limit: 100})
	if err != nil || len(page.Data) != 2 || !page.Data[1].IngestedAt.After(page.Data[0].IngestedAt) {
		t.Fatal("clock rollback reordered ingestion", page, err)
	}
}
