//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntegrationSchemaParkingPolicyRecoveryAndRestart(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	base := fixture(t, "volcanic-v1.json")[0]

	records := []Object{clone(base), clone(base), clone(base), clone(base)}
	for i, id := range []string{"original", "renamed", "string-count", "missing-reference"} {
		records[i]["report_id"], _ = json.Marshal(id)
	}

	records[1]["ash_height_m"] = records[1]["ash_column_height_m"]
	delete(records[1], "ash_column_height_m")
	records[2]["eruption_count_24h"] = json.RawMessage(`"7"`)
	delete(records[3], "volcano_id")
	body, _ := json.Marshal(records)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer source-secret" {
			w.WriteHeader(401)
			return
		}

		_, _ = w.Write(body)
	}))
	defer server.Close()

	policyPath := filepath.Join(t.TempDir(), "schema-policy.json")
	if err := os.WriteFile(policyPath, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	p := &Poller{Source: "PVMBG", BaseURL: server.URL, Credential: "source-secret", Store: &EpochIngest{Data: s, Source: "PVMBG", WriterEpoch: 1}, PolicyFile: policyPath, Client: server.Client(), Timeout: time.Second, Log: testLog()}
	for range 2 {
		if err := p.Poll(ctx, "schema-test"); err != nil {
			t.Fatal(err)
		}
	}

	page, err := s.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 3 {
		t.Fatalf("rejected records lost or duplicated: %d %v", len(page.Items), err)
	}

	for _, m := range page.Items {
		if m.Retryable || m.Status != "parked" || m.CorrelationID != "schema-test" || bytes.Contains(m.Raw, []byte("source-secret")) {
			t.Fatal("invalid parking metadata", m.MessageID)
		}

		var decoded Object
		if json.Unmarshal(m.Raw, &decoded) != nil {
			t.Fatal("raw payload changed")
		}

		ref, _ := textField(decoded, "report_id")
		if ref != m.SourceRefID {
			t.Fatal("parking reference lost")
		}
	}
	// A process restart reloads PostgreSQL, rather than an in-memory quarantine.
	reopened, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4, defaultDBConnectTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	p.Store = &EpochIngest{Data: reopened, Source: "PVMBG", WriterEpoch: 1}

	decode := func(m ParkedMessage) (HazardEvent, error) { return p.DecodeParked(ctx, m) }
	if n, err := reopened.RetryDue(ctx, "PVMBG", 1, 100, decode); err != nil || n != 0 {
		t.Fatal("permanent errors retried automatically", n, err)
	}

	if err := os.WriteFile(policyPath, []byte(`{"accept_ash_rename":true,"accept_string_count":true,"volcano_by_report":{"missing-reference":"merapi"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var (
		total atomic.Int32
		wg    sync.WaitGroup
	)
	for range 2 {
		wg.Go(func() {
			n, err := reopened.Replay(ctx, "PVMBG", 1, 100, decode)
			if err != nil {
				t.Error(err)
			}

			total.Add(int32(n))
		})
	}

	wg.Wait()

	if total.Load() != 3 {
		t.Fatal("concurrent recovery count", total.Load())
	}

	page, err = reopened.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("recovered messages still pending", err)
	}

	hazards, err := reopened.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || len(hazards.Data) != 4 {
		t.Fatal("canonical effects", len(hazards.Data), err)
	}

	for _, h := range hazards.Data {
		if h.Revision != 1 {
			t.Fatal("duplicate effect", h.SourceRefID, h.Revision)
		}
	}

	t.Log("strict modes 3/4/5 parked durably, explicit policy recovered each once after restart")
}

func TestIntegrationMalformedBoundaryKeepsGoodRecords(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	base := fixture(t, "volcanic-v1.json")[0]
	good, _ := json.Marshal(base)
	badUTF := []byte(`{"report_id":"bad-utf8","text":"`)
	badUTF = append(badUTF, 0xff)
	badUTF = append(badUTF, []byte(`"}`)...)
	deep := []byte(`{"report_id":"deep","extra":` + strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33) + "}")
	body := bytes.Join([][]byte{[]byte("["), good, []byte(","), badUTF, []byte(","), deep, []byte(",{\"unfinished\":")}, nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer server.Close()

	p := &Poller{Source: "PVMBG", BaseURL: server.URL, Store: &EpochIngest{Data: s, Source: "PVMBG", WriterEpoch: 1}, Client: server.Client(), Timeout: time.Second, Log: testLog()}
	if err := p.Poll(ctx, "malformed-test"); err != nil {
		t.Fatal(err)
	}

	page, err := s.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 3 {
		t.Fatal("boundary evidence lost", len(page.Items), err)
	}

	keptInvalidUTF := false
	for _, m := range page.Items {
		keptInvalidUTF = keptInvalidUTF || bytes.Equal(m.Raw, badUTF)
	}

	if !keptInvalidUTF {
		t.Fatal("invalid UTF-8 bytes were replaced")
	}

	hazards, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || len(hazards.Data) != 1 {
		t.Fatal("good prefix was lost", len(hazards.Data), err)
	}

	t.Log("invalid UTF-8, excessive depth and truncated body parked; valid record committed")
}

func TestIntegrationBMKGBoundaryAndSchemaParkingSurviveRestart(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	base := fixture(t, "seismic.json")[0]
	good, _ := json.Marshal(base)
	bad := clone(base)
	bad["event_id"] = json.RawMessage(`"bad-magnitude"`)
	bad["magnitude"] = json.RawMessage(`"large"`)
	rejected, _ := json.Marshal(bad)

	invalidUTF := append([]byte(`{"text":"`), 0xff)
	invalidUTF = append(invalidUTF, []byte(`"}`)...)
	deep := []byte(`{"extra":` + strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33) + "}")
	body := bytes.Join([][]byte{[]byte("["), good, []byte(","), rejected, []byte(","), invalidUTF, []byte(","), deep, []byte(`,{"unfinished":`)}, nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-BMKG-Key") != "bmkg-private-test" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.URL.Path == "/tsunami-warnings" {
			_, _ = w.Write([]byte("[]"))
			return
		}

		_, _ = w.Write(body)
	}))
	defer server.Close()

	p := &Poller{Source: "BMKG", BaseURL: server.URL, Credential: "bmkg-private-test", Store: &EpochIngest{Data: s, Source: "BMKG", WriterEpoch: 1}, Client: server.Client(), Timeout: time.Second, Log: testLog()}
	for range 2 {
		if err := p.Poll(ctx, "bmkg-boundary"); err != nil {
			t.Fatal(err)
		}
	}

	reopened, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4, defaultDBConnectTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	page, err := reopened.ListUnprocessed(ctx, "BMKG", "", 100)
	if err != nil || len(page.Items) != 4 {
		t.Fatalf("BMKG parking lost or duplicated after restart: %d %v", len(page.Items), err)
	}

	keptUTF, keptRejected := false, false
	for _, message := range page.Items {
		keptUTF = keptUTF || bytes.Equal(message.Raw, invalidUTF)

		keptRejected = keptRejected || bytes.Equal(message.Raw, rejected)
		if message.CorrelationID != "bmkg-boundary" || bytes.Contains(message.Raw, []byte("bmkg-private-test")) {
			t.Fatal("unsafe or missing metadata")
		}
	}

	hazards, err := reopened.Read(ctx, Query{Source: "BMKG", Limit: 100})
	if err != nil || len(hazards.Data) != 1 || !keptUTF || !keptRejected {
		t.Fatal("BMKG good record or exact rejected bytes lost", err)
	}

	if hazards.Data[0].Revision != 1 {
		t.Fatal("duplicate poll created another business effect")
	}

	t.Log("BMKG valid prefix committed; schema, UTF-8, depth and truncation parked durably once across restart")
}

func TestIntegrationNetworkCutBodyRecoversAfterCleanRefetch(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	good, _ := json.Marshal(fixture(t, "volcanic-v1.json")[0])
	full := append(append([]byte("["), good...), ']')

	var calls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			// Promise more bytes than are sent, so the client sees the connection drop mid-body.
			w.Header().Set("Content-Length", "100000")
			_, _ = w.Write(append(append([]byte("["), good...), ','))

			return
		}

		_, _ = w.Write(full)
	}))
	defer server.Close()

	p := &Poller{Source: "PVMBG", BaseURL: server.URL, Store: &EpochIngest{Data: s, Source: "PVMBG", WriterEpoch: 1}, Client: server.Client(), Timeout: time.Second, Log: testLog()}
	decode := func(m ParkedMessage) (HazardEvent, error) { return p.DecodeParked(ctx, m) }

	if err := p.Poll(ctx, "cut-test"); err != nil {
		t.Fatal(err)
	}

	page, err := s.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 1 || !page.Items[0].Retryable || page.Items[0].Status != parkingRetrying {
		t.Fatal("network cut not parked as retrying", page, err)
	}

	// Before any clean read the prefix cannot recover on its own.
	if n, err := s.ReplayReason(ctx, "PVMBG", 1, 100, "", decode); err != nil || n != 0 {
		t.Fatal("cut body recovered without a refetch", n, err)
	}

	if err = p.Poll(ctx, "cut-test-2"); err != nil {
		t.Fatal(err)
	}

	if n, err := s.ReplayReason(ctx, "PVMBG", 1, 100, "", decode); err != nil || n != 1 {
		t.Fatal("clean refetch did not recover the cut body", n, err)
	}

	page, err = s.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("recovered cut body still listed", page, err)
	}

	hazards, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || len(hazards.Data) != 1 || hazards.Data[0].Revision != 1 {
		t.Fatal("refetch lost or duplicated the record", hazards, err)
	}
}
