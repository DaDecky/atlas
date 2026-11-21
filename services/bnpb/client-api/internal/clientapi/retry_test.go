package clientapi

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// aggregatorReplies answers Auth as active and plays the given Aggregator outcomes in order.
func aggregatorReplies(t *testing.T, outcomes ...int) (*API, string, *int) {
	t.Helper()

	a, key := testAPI(t, 200, `{"active":true}`, 200, `{"data":[],"sources":[]}`, nil)
	calls := 0
	a.Client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, status := `{"active":true}`, http.StatusOK

		if r.URL.Host == "aggregator:8083" {
			status = outcomes[calls]
			calls++

			if status == 0 {
				return nil, errors.New("connection reset")
			}

			body = `{"data":[],"sources":[]}`
		}

		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})

	return a, testToken(t, key, "hazards:raw", time.Now().Add(time.Minute)), &calls
}

func TestFailedReadIsRetriedOnce(t *testing.T) {
	a, token, calls := aggregatorReplies(t, 0, http.StatusOK)

	if w := invoke(a, "/hazards", token); w.Code != http.StatusOK || *calls != 2 {
		t.Fatalf("status %d after %d calls, want 200 after 2", w.Code, *calls)
	}
}

func TestTwoFailedReadsAnswerUnavailable(t *testing.T) {
	a, token, calls := aggregatorReplies(t, 0, 0)

	if w := invoke(a, "/hazards", token); w.Code != http.StatusServiceUnavailable || *calls != 2 {
		t.Fatalf("status %d after %d calls, want 503 after 2", w.Code, *calls)
	}
}

func TestUnavailableAnswerIsNotRetried(t *testing.T) {
	a, token, calls := aggregatorReplies(t, http.StatusServiceUnavailable)

	invoke(a, "/hazards", token)

	if *calls != 1 {
		t.Fatalf("aggregator called %d times, want 1", *calls)
	}
}
