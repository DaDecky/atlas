package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"atlas/services/bnpb/aggregator/internal/decode"
)

// SchemaPolicy contains explicit operator decisions; the default never guesses.
type SchemaPolicy struct {
	AcceptAshRename   bool              `json:"accept_ash_rename"`
	AcceptStringCount bool              `json:"accept_string_count"`
	VolcanoByReport   map[string]string `json:"volcano_by_report,omitempty"`
}

type sourceRecord struct {
	Object Object
	Raw    []byte
}

func ReadSchemaPolicy(path string) (SchemaPolicy, error) {
	var policy SchemaPolicy
	if path == "" {
		return policy, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return policy, errors.New("schema policy unavailable")
	}
	defer file.Close()

	raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return policy, errors.New("schema policy unreadable or exceeds limit")
	}

	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return policy, errors.New("schema policy must be an object")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err = decoder.Decode(&policy); err != nil {
		return policy, errors.New("invalid schema policy")
	}

	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return policy, errors.New("trailing schema policy data")
	}

	for ref, volcano := range policy.VolcanoByReport {
		if _, ok := Volcanoes[volcano]; !ok || ref == "" || len(ref) > 256 {
			return policy, errors.New("invalid report reference in schema policy")
		}
	}

	return policy, nil
}

func (p SchemaPolicy) MapVolcanic(o Object, now time.Time) (HazardEvent, error) {
	normalized := make(Object, len(o)+1)
	for k, v := range o {
		normalized[k] = v
	}

	if raw, ok := o["ash_height_m"]; ok && p.AcceptAshRename {
		if original, exists := o["ash_column_height_m"]; exists && !bytes.Equal(original, raw) {
			return HazardEvent{}, errors.New("conflicting ash fields")
		}

		normalized["ash_column_height_m"] = raw
	}

	if raw := bytes.TrimSpace(o["eruption_count_24h"]); len(raw) > 0 && raw[0] == '"' && p.AcceptStringCount {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return HazardEvent{}, errors.New("invalid eruption count")
		}

		count, err := strconv.ParseInt(value, 10, 64)
		if err != nil || count < 0 {
			return HazardEvent{}, errors.New("invalid eruption count")
		}

		normalized["eruption_count_24h"], _ = json.Marshal(count)
	}

	if _, exists := o["volcano_id"]; !exists {
		ref, _ := textField(o, "report_id")
		if volcano, known := p.VolcanoByReport[ref]; known {
			normalized["volcano_id"], _ = json.Marshal(volcano)
		}
	}

	return MapVolcanic(normalized, now)
}

func (p *Poller) park(ctx context.Context, path, id string, raw []byte, code, reason string, issue *decode.BodyError) error {
	ref := ""

	var object Object
	if utf8.Valid(raw) && json.Unmarshal(raw, &object) == nil && object != nil {
		key := pollerSources[p.Source].idField

		if path == "/tsunami-warnings" {
			key = "related_event_id"
		}

		ref, _ = textField(object, key)
	}

	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])

	m := ParkedMessage{
		Source: p.Source, SourceRefID: ref, Raw: raw, RawLength: int64(len(raw)), RawSHA256: digest,
		ReasonCode: code, Reason: reason, ReceivedAt: time.Now().UTC(), CorrelationID: id,
	}
	if issue != nil {
		m.RawLength, m.RawSHA256, m.RawCut, m.Retryable = issue.Length, issue.SHA256, issue.Cut, issue.Retryable
	}

	if path == "/tsunami-warnings" {
		m.ReasonCode = "warning_" + m.ReasonCode
	}

	identity := sha256.Sum256([]byte(p.Source + "\x00" + path + "\x00" + m.RawSHA256))

	m.MessageID = hex.EncodeToString(identity[:])
	if err := p.Store.Park(ctx, m); err != nil {
		return errors.New("failed-message parking unavailable")
	}

	return nil
}

func (p *Poller) DecodeParked(ctx context.Context, m ParkedMessage) (HazardEvent, error) {
	if m.RawCut || m.Source != p.Source || len(m.Raw) > defaultMaxSourceBytes {
		return HazardEvent{}, errors.New("message requires source repair")
	}

	var object Object

	reader := io.MultiReader(strings.NewReader("["), bytes.NewReader(m.Raw), strings.NewReader("]"))

	err := decode.Array(reader, decode.Limits{BodyBytes: defaultMaxSourceBytes + 2, Records: 1}, func(record decode.Record) error {
		if record.Code != "" {
			return errors.New("parked record fails boundary validation")
		}

		object = Object(record.Object)

		return nil
	})
	if err != nil || object == nil {
		return HazardEvent{}, errors.New("parked record is not a valid object")
	}

	if strings.HasPrefix(m.ReasonCode, "warning_") {
		_, warning, err := mapWarning(object)
		if err != nil {
			return HazardEvent{}, err
		}

		stored, err := p.Store.WarningEvents(ctx, []string{warning.RelatedID})
		if err != nil || len(stored) != 1 {
			return HazardEvent{}, errors.New("warning target unavailable")
		}

		return ApplyWarnings(stored[0], []Warning{warning}), nil
	}

	src := pollerSources[p.Source]

	policy, err := p.schemaPolicy(src)
	if err != nil {
		return HazardEvent{}, err
	}

	return src.mapRecord(object, nil, policy, time.Now().UTC())
}
