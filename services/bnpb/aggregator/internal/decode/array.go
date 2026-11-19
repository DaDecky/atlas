// Package decode bounds and isolates untrusted JSON source records.
package decode

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"unicode/utf8"
)

type Limits struct {
	BodyBytes, PrefixBytes int
	Depth, Records         int
}

type Record struct {
	Raw    []byte
	Object map[string]json.RawMessage
	Code   string
}

// BodyError retains the complete received body, or a bounded prefix when cut.
// SHA256 and Length cover all received bytes, including the drained tail.
type BodyError struct {
	Code      string
	Raw       []byte
	Length    int64
	SHA256    string
	Cut       bool
	Retryable bool
}

type capture struct {
	r     io.Reader
	hash  hash.Hash
	raw   []byte
	count int64
	limit int
}

type arrayReader struct {
	input *bufio.Reader
	trace *capture
	limit Limits
}

var errBodyLimit = errors.New("body limit exceeded")

func (e *BodyError) Error() string { return "source body: " + e.Code }

func (c *capture) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		_, _ = c.hash.Write(p[:n])
		c.count += int64(n)
		keep := min(n, c.limit-len(c.raw))
		c.raw = append(c.raw, p[:keep]...)
	}

	return n, err
}

func (a *arrayReader) next() (byte, error) {
	b, err := a.input.ReadByte()
	if a.trace.count > int64(a.limit.BodyBytes) {
		return 0, errBodyLimit
	}

	return b, err
}

func space(b byte) bool { return b == ' ' || b == '\t' || b == '\r' || b == '\n' }

func (a *arrayReader) nonspace() (byte, error) {
	for {
		b, err := a.next()
		if err != nil || !space(b) {
			return b, err
		}
	}
}

func (a *arrayReader) failure(code string, cause error) error {
	_, drainErr := io.Copy(io.Discard, a.trace)
	cut := a.trace.count > int64(a.limit.BodyBytes)

	raw := a.trace.raw
	if cut {
		code = "body_too_large"
		raw = raw[:min(len(raw), a.limit.PrefixBytes)]
	} else if !utf8.Valid(raw) {
		code = "invalid_utf8"
	}

	return &BodyError{
		Code: code, Raw: append([]byte(nil), raw...), Length: a.trace.count,
		SHA256: hex.EncodeToString(a.trace.hash.Sum(nil)), Cut: cut,
		Retryable: drainErr != nil || cause != nil && !errors.Is(cause, io.EOF) && !errors.Is(cause, errBodyLimit),
	}
}

// value scans one element without asking encoding/json to allocate a deep tree.
// Balanced but invalid elements can be parked without losing the next element.
func (a *arrayReader) value(first byte) ([]byte, byte, bool, error) {
	raw := make([]byte, 0, 512)
	depth, quoted, escaped, deep := 0, false, false, false

	b := first
	for {
		if !quoted && depth == 0 && (b == ',' || b == ']') {
			return raw, b, deep, nil
		}

		raw = append(raw, b)

		if quoted {
			switch {
			case escaped:
				escaped = false
			case b == '\\':
				escaped = true
			case b == '"':
				quoted = false
			}
		} else {
			switch b {
			case '"':
				quoted = true
			case '{', '[':
				depth++
				deep = deep || depth > a.limit.Depth
			case '}', ']':
				if depth > 0 {
					depth--
				}
			}
		}

		var err error

		b, err = a.next()
		if err != nil {
			return raw, 0, deep, err
		}
	}
}

func record(raw []byte, deep bool) Record {
	r := Record{Raw: raw}
	switch {
	case !utf8.Valid(raw):
		r.Code = "invalid_utf8"
	case deep:
		r.Code = "json_depth_exceeded"
	case json.Unmarshal(raw, &r.Object) != nil || r.Object == nil:
		r.Code = "invalid_json_record"
	}

	return r
}

// Array calls emit once for each complete element, including rejected elements.
// An unreadable envelope/tail is returned for durable parking by the caller.
// A storage error from emit is returned unchanged and must not be acknowledged.
func Array(input io.Reader, limits Limits, emit func(Record) error) error {
	if limits.BodyBytes <= 0 {
		limits.BodyBytes = 4 << 20
	}

	if limits.PrefixBytes <= 0 {
		limits.PrefixBytes = 64 << 10
	}

	if limits.Depth <= 0 {
		limits.Depth = 32
	}

	if limits.Records <= 0 {
		limits.Records = 1000
	}

	c := &capture{r: input, hash: sha256.New(), limit: limits.BodyBytes}
	a := &arrayReader{input: bufio.NewReader(c), trace: c, limit: limits}

	b, err := a.nonspace()
	if err != nil || b != '[' {
		return a.failure("invalid_json_array", err)
	}

	count, afterComma := 0, false

	for {
		b, err = a.nonspace()
		if err != nil {
			return a.failure("truncated_json", err)
		}

		if b == ']' {
			if afterComma {
				return a.failure("invalid_json_array", nil)
			}

			break
		}

		count++
		if count > limits.Records {
			return a.failure("record_limit_exceeded", nil)
		}

		raw, delimiter, deep, readErr := a.value(b)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return a.failure("body_read_failed", readErr)
		}

		r := record(raw, deep)
		if readErr != nil && r.Code != "" {
			return a.failure("truncated_json", readErr)
		}

		if err = emit(r); err != nil {
			return err
		}

		if readErr != nil {
			return a.failure("truncated_json", readErr)
		}

		if delimiter == ']' {
			break
		}

		afterComma = true
	}

	_, err = a.nonspace()
	if errors.Is(err, io.EOF) {
		return nil
	}

	return a.failure("trailing_json", err)
}
