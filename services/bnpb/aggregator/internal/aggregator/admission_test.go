package aggregator

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestMediaQueriesStopAtReservedShare(t *testing.T) {
	s := &readStub{wait: true, entered: make(chan struct{}, 8)}
	a := &API{Store: s, Token: "test-internal", Timeout: 2 * time.Second, Admission: NewAdmission(4, 25), Log: testLog()}
	h := a.Handler()
	query := func(class string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/internal/hazards", nil)
		r.Header.Set("Authorization", "Bearer test-internal")
		r.Header.Set("X-Request-Class", class)

		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		return w
	}

	var held sync.WaitGroup
	for range 3 {
		held.Go(func() { query("media") })
		<-s.entered
	}

	if w := query("media"); w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("fourth media query = %d, want 429 with Retry-After", w.Code)
	}

	held.Go(func() { query("priority") })

	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("priority query was not admitted into the reserved share")
	}

	held.Wait()

	if a.Admission.inUse != 0 {
		t.Fatal("admission slot leaked")
	}
}
