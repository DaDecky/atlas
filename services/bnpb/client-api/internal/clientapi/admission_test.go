package clientapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMediaRefusedWhileFieldTeamAdmitted(t *testing.T) {
	a, key := testAPI(t, 200, `{"active":true}`, 200, `{"data":[],"sources":[]}`, nil)
	a.Config.RequestCapacity, a.Config.FieldReservedPercent = 4, 25

	entered, release := make(chan string, 8), make(chan struct{})
	a.Client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"data":[],"sources":[]}`

		if r.URL.Host == "auth:8084" {
			entered <- r.Header.Get("X-Correlation-ID")

			<-release

			body = `{"active":true}`
		}

		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})

	h := a.Handler()
	media := testToken(t, key, "hazards:summary", time.Now().Add(time.Minute))
	field := testToken(t, key, "hazards:raw reports:write", time.Now().Add(time.Minute))
	request := func(token, id string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/hazards", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Correlation-ID", id)

		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		return w
	}

	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()

	var held sync.WaitGroup
	for _, id := range []string{"media-1", "media-2", "media-3"} {
		held.Go(func() { request(media, id) })
		<-entered
	}

	fourth := make(chan *httptest.ResponseRecorder, 1)
	go func() { fourth <- request(media, "media-4") }()

	select {
	case id := <-entered:
		t.Fatalf("admitted %s past the media limit", id)
	case w := <-fourth:
		if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" {
			t.Fatalf("fourth media request = %d, want 429 with Retry-After", w.Code)
		}
	}

	fieldDone := make(chan int, 1)
	go func() { fieldDone <- request(field, "field-1").Code }()

	if id := <-entered; id != "field-1" {
		t.Fatalf("admitted %s, want the field team request", id)
	}

	unblock()
	held.Wait()

	if code := <-fieldDone; code != http.StatusOK {
		t.Fatalf("field team request = %d, want 200", code)
	}
}

func TestAggregatorReceivesRequestClass(t *testing.T) {
	var classes []string

	a, key := testAPI(t, 200, `{"active":true}`, 200, `{"data":[],"sources":[]}`, func(r *http.Request) {
		if r.URL.Host == "aggregator:8083" {
			classes = append(classes, r.Header.Get("X-Request-Class"))
		}
	})

	for _, scope := range []string{"hazards:summary", "hazards:raw"} {
		if w := invoke(a, "/hazards", testToken(t, key, scope, time.Now().Add(time.Minute))); w.Code != http.StatusOK {
			t.Fatalf("%s request = %d", scope, w.Code)
		}
	}

	if strings.Join(classes, ",") != "media,priority" {
		t.Fatalf("classes = %v, want media then priority", classes)
	}
}

func TestReservedShareOutsideRangeFailsAtStart(t *testing.T) {
	validTestEnv(t)
	t.Setenv("FIELD_TEAM_RESERVED_PERCENT", "95")

	if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "FIELD_TEAM_RESERVED_PERCENT") {
		t.Fatalf("error = %v, want the reserved share rejected", err)
	}
}
