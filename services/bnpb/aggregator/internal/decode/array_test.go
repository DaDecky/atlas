package decode

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestArrayIsolatesBadRecordsAndRetainsTheirBytes(t *testing.T) {
	bad := []byte("{\"bad\":\"\xff\"}")
	body := append([]byte(`[{"ok":1},`), bad...)
	body = append(body, []byte(`,{"ok":2}]`)...)

	var got []Record

	err := Array(bytes.NewReader(body), Limits{}, func(r Record) error { got = append(got, r); return nil })
	if err != nil || len(got) != 3 || got[0].Code != "" || got[2].Code != "" || got[1].Code != "invalid_utf8" || !bytes.Equal(got[1].Raw, bad) {
		t.Fatalf("isolation: records=%+v error=%v", got, err)
	}
}

func TestArrayDepthBoundaryAndEscapedDelimiters(t *testing.T) {
	for _, depth := range []int{32, 33, 20000} {
		raw := strings.Repeat(`{"x":`, depth) + `"escaped \\\" ],}"` + strings.Repeat("}", depth)

		var records []Record

		err := Array(strings.NewReader("["+raw+`,{"ok":true}]`), Limits{}, func(r Record) error { records = append(records, r); return nil })
		if err != nil || len(records) != 2 || records[1].Code != "" {
			t.Fatalf("depth %d: records=%d error=%v", depth, len(records), err)
		}

		want := ""
		if depth > 32 {
			want = "json_depth_exceeded"
		}

		if records[0].Code != want {
			t.Fatalf("depth %d: code=%q want=%q", depth, records[0].Code, want)
		}
	}
}

func TestArrayParksTruncatedTailAfterCompleteRecords(t *testing.T) {
	body := `[{"ok":1},{"broken":`

	var records []Record

	err := Array(strings.NewReader(body), Limits{}, func(r Record) error { records = append(records, r); return nil })

	var issue *BodyError
	if !errors.As(err, &issue) || issue.Code != "truncated_json" || string(issue.Raw) != body || len(records) != 1 || records[0].Code != "" {
		t.Fatalf("records=%+v error=%+v", records, err)
	}
}

func TestArrayOversizeDrainsAndHashesTheWholeResponse(t *testing.T) {
	body := `[{"value":"` + strings.Repeat("x", 2<<20) + `"}]`
	err := Array(strings.NewReader(body), Limits{BodyBytes: 1 << 20, PrefixBytes: 64 << 10}, func(Record) error { return nil })

	var issue *BodyError

	sum := sha256.Sum256([]byte(body))
	if !errors.As(err, &issue) || issue.Code != "body_too_large" || !issue.Cut || len(issue.Raw) != 64<<10 || issue.Length != int64(len(body)) || issue.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("oversize metadata: %+v", err)
	}
}

func TestArrayRejectsInvalidEnvelopesAndDoesNotSwallowStorageErrors(t *testing.T) {
	for _, body := range []string{`null`, `{}`, `[{},]`, `[{}] {}`, `[`} {
		if err := Array(strings.NewReader(body), Limits{}, func(Record) error { return nil }); err == nil {
			t.Fatalf("accepted invalid envelope %q", body)
		}
	}

	failure := errors.New("storage failed")

	err := Array(strings.NewReader(`[{},{}]`), Limits{}, func(Record) error { return failure })
	if !errors.Is(err, failure) {
		t.Fatalf("storage failure became %v", err)
	}
}
