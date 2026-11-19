package satellite

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"atlas/services/bnpb/aggregator/internal/decode"
)

type SSEEvent struct {
	Type       string
	Offset     int64
	Data       []byte
	ReceivedAt time.Time
}

const MaxFrameBytes = 64 << 10

// ReadSSE bounds both lines and complete events. Comments do not produce an
// offset; the transport's activity reader still counts them as heartbeats.
func ReadSSE(ctx context.Context, reader io.Reader, emit func(SSEEvent) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), MaxFrameBytes+1)

	kind, id := "", ""
	data := []byte{}
	size := 0

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}

		line := scanner.Bytes()
		if !utf8.Valid(line) {
			return errors.New("SSE contains invalid UTF-8")
		}

		if len(line) == 0 {
			if len(data) > 0 {
				offset, err := strconv.ParseInt(id, 10, 64)
				if err != nil || offset < 0 {
					return errors.New("SSE event requires a nonnegative numeric ID")
				}

				if err = emit(SSEEvent{Type: kind, Offset: offset, Data: bytes.TrimSuffix(data, []byte{'\n'}), ReceivedAt: time.Now()}); err != nil {
					return err
				}
			}

			kind, data, size = "", nil, 0

			continue
		}

		if line[0] == ':' {
			continue
		}

		size += len(line)
		if size > MaxFrameBytes {
			return errors.New("SSE event exceeds size limit")
		}

		field, value, _ := strings.Cut(string(line), ":")
		value = strings.TrimPrefix(value, " ")

		switch field {
		case "event":
			kind = value
		case "id":
			if strings.ContainsRune(value, 0) {
				return errors.New("invalid SSE ID")
			}

			id = value
		case "data":
			data = append(data, value...)
			data = append(data, '\n')
		}
	}

	if err := scanner.Err(); err != nil {
		return errors.New("SSE read failed or line exceeds limit")
	}
	// An incomplete final event is deliberately uncommitted and fetched again.
	return io.EOF
}

// DecodeFeed is the explicit supported wire contract. Detection/update payloads
// may be a record or {"detection": record}; a retract carries that identity's
// key or its five identity fields. Numeric HHMM and VIIRS l/n/h values are
// normalized. A live group-token probe must confirm the assistants' payload.
func DecodeFeed(event SSEEvent) (Frame, error) {
	frame := Frame{Offset: event.Offset, Type: event.Type, Raw: append(json.RawMessage(nil), event.Data...)}

	var object map[string]json.RawMessage

	err := decode.Array(io.MultiReader(strings.NewReader("["), bytes.NewReader(event.Data), strings.NewReader("]")), decode.Limits{BodyBytes: MaxFrameBytes + 2, Records: 1}, func(record decode.Record) error {
		if record.Code != "" {
			return errors.New("SSE JSON fails boundary validation")
		}

		object = record.Object

		return nil
	})
	if err != nil || object == nil {
		return frame, errors.New("invalid SSE JSON object")
	}

	if event.Type == "heartbeat" {
		return frame, nil
	}

	if nested, exists := object["detection"]; exists {
		if json.Unmarshal(nested, &object) != nil || object == nil {
			return frame, errors.New("invalid nested detection")
		}
	}

	if event.Type == "retract" && object["key"] != nil {
		if json.Unmarshal(object["key"], &frame.Key) != nil || frame.Key == "" {
			return frame, errors.New("invalid retract key")
		}

		return frame, nil
	}

	required := []string{"satellite", "acq_date", "acq_time", "latitude", "longitude"}
	if event.Type != "retract" {
		required = append(required, "confidence", "frp")
	}

	for _, key := range required {
		if raw, exists := object[key]; !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return frame, errors.New("missing detection field")
		}
	}

	if raw := object["acq_time"]; len(raw) > 0 && raw[0] != '"' {
		var n int
		if json.Unmarshal(raw, &n) != nil || n < 0 || n > 2359 {
			return frame, errors.New("invalid numeric HHMM")
		}

		object["acq_time"], _ = json.Marshal(fmt.Sprintf("%04d", n))
	}

	raw, _ := json.Marshal(object)

	var d Detection
	if json.Unmarshal(raw, &d) != nil {
		return frame, errors.New("invalid detection types")
	}

	switch d.Confidence {
	case "l":
		d.Confidence = "low"
	case "n":
		d.Confidence = "nominal"
	case "h":
		d.Confidence = "high"
	}

	if event.Type == "retract" {
		// Retract identity records need no measurements.
		d.Confidence, d.FRP = "nominal", 0
		frame.Key = d.Key()
	} else {
		frame.Detection = &d
	}

	if d.Validate() != nil {
		return frame, errors.New("invalid detection values")
	}

	return frame, nil
}
