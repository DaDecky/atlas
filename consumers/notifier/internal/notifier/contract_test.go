package notifier

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// sharedContract is the one file that Auth, the Client API, the Aggregator, and the notifier
// each test against, so a change on one side fails the tests of every other side.
type sharedContract struct {
	Queues map[string]struct {
		Durable   bool           `json:"durable"`
		Arguments map[string]any `json:"arguments"`
	} `json:"queues"`
	Scopes  []string          `json:"scopes"`
	Sources map[string]string `json:"sources"`
	Claims  []string          `json:"access_token_claims"`
}

func readContract(t *testing.T) sharedContract {
	t.Helper()

	raw, err := os.ReadFile("../../../../contracts/shared.json")
	if err != nil {
		t.Fatal(err)
	}

	var c sharedContract
	if err = json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}

	return c
}

// normalized turns a queue argument table into JSON values, so int32 and float64 compare.
func normalized(t *testing.T, table map[string]any) map[string]any {
	t.Helper()

	raw, err := json.Marshal(table)
	if err != nil {
		t.Fatal(err)
	}

	out := map[string]any{}
	if err = json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}

	// A table with no arguments marshals to null, which is the same contract as {}.
	if out == nil {
		out = map[string]any{}
	}

	return out
}

func TestAlertQueueMatchesContract(t *testing.T) {
	want := readContract(t).Queues[queue]

	if !want.Durable || !reflect.DeepEqual(normalized(t, queueArgs), want.Arguments) {
		t.Fatalf("declared %v, contract %v", queueArgs, want.Arguments)
	}
}
