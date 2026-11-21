package aggregator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpsReplayReasonContract(t *testing.T) {
	calls := 0
	api := (&OpsAPI{Token: "internal-test-token", Capacity: defaultOpsCapacity, Replay: map[string]ReplaySource{
		"PVMBG": func(_ context.Context, limit int, reason string) (int, error) {
			calls++

			if limit != 100 || reason != "schema_violation" {
				t.Fatalf("unexpected dispatch: limit %d, reason %q", limit, reason)
			}

			return 2, nil
		},
	}}).Handler()

	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"source":"PVMBG","reason_code":"schema_violation"}`, http.StatusOK},
		{`{"source":"PVMBG","reason_code":"*"}`, http.StatusBadRequest},
	} {
		request := httptest.NewRequest(http.MethodPost, "/internal/ops/replay", strings.NewReader(tc.body))
		request.Header.Set("Authorization", "Bearer internal-test-token")

		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)

		if response.Code != tc.code {
			t.Fatalf("response %d, want %d: %s", response.Code, tc.code, response.Body)
		}
	}

	if calls != 1 {
		t.Fatalf("replay dispatched %d times, want 1", calls)
	}
}
