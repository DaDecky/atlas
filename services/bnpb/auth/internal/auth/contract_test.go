package auth

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
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

	raw, err := os.ReadFile("../../../../../contracts/shared.json")
	if err != nil {
		t.Fatal(err)
	}

	var c sharedContract
	if err = json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}

	return c
}

func jsonTags(value any) []string {
	tags := []string{}

	kind := reflect.TypeOf(value)
	for i := range kind.NumField() {
		tags = append(tags, kind.Field(i).Tag.Get("json"))
	}

	sort.Strings(tags)

	return tags
}

func TestTokenMatchesContract(t *testing.T) {
	validTestEnv(t)

	c := readContract(t)
	sort.Strings(c.Claims)

	if got := jsonTags(AccessClaims{}); !reflect.DeepEqual(got, c.Claims) {
		t.Errorf("claims %v, contract %v", got, c.Claims)
	}

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}

	known := map[string]bool{}
	for _, scope := range c.Scopes {
		known[scope] = true
	}

	for _, client := range config.Clients {
		for _, scope := range strings.Fields(client.Scope) {
			if !known[scope] {
				t.Errorf("client %s gets scope %s outside the contract", client.ID, scope)
			}
		}
	}
}
