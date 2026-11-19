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

// DecodeFeed accepts detection records and keyed corrections. The practice feed
// wraps detections and sends updates as field changes rather than full records.
func DecodeFeed(event SSEEvent) (Frame, error) {
	frame := Frame{Offset: event.Offset, Type: event.Type, Raw: append(json.RawMessage(nil), event.Data...)}

	object, err := decodeFeedObject(event.Data)
	if err != nil || event.Type == "heartbeat" {
		return frame, err
	}

	if raw, exists := object["offset"]; exists {
		var offset int64
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &offset) != nil || offset != event.Offset {
			return frame, errors.New("SSE offset and payload offset disagree")
		}
	}

	if changes, exists := object["changes"]; event.Type == "update" && exists {
		return decodeFeedUpdate(frame, object, changes)
	}

	if nested, exists := object["detection"]; exists {
		if json.Unmarshal(nested, &object) != nil || object == nil {
			return frame, errors.New("invalid nested detection")
		}
	}

	if event.Type == "retract" && object["key"] != nil {
		frame.Key, err = decodeFeedKey(object["key"])
		return frame, err
	}

	d, err := decodeFeedDetection(object, event.Type == "retract")
	if err != nil {
		return frame, err
	}

	if event.Type == "retract" {
		frame.Key = d.Key()
	} else {
		frame.Detection = &d
	}

	return frame, nil
}

// decodeFeedObject reads one JSON object through the boundary checks of the decoder.
func decodeFeedObject(data []byte) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage

	body := io.MultiReader(strings.NewReader("["), bytes.NewReader(data), strings.NewReader("]"))

	err := decode.Array(body, decode.Limits{BodyBytes: MaxFrameBytes + 2, Records: 1}, func(record decode.Record) error {
		if record.Code != "" {
			return errors.New("SSE JSON fails boundary validation")
		}

		object = record.Object

		return nil
	})
	if err != nil || object == nil {
		return nil, errors.New("invalid SSE JSON object")
	}

	return object, nil
}

func decodeFeedUpdate(frame Frame, object map[string]json.RawMessage, changes json.RawMessage) (Frame, error) {
	key, err := decodeFeedKey(object["key"])
	if err != nil || object["detection"] != nil {
		return frame, errors.New("invalid partial update identity")
	}

	patch, err := decodeFeedPatch(changes)
	if err != nil {
		return frame, err
	}

	frame.Key, frame.Changes = key, &patch

	return frame, nil
}

// decodeFeedDetection checks the required fields and builds a detection. A retraction
// identifies a record only, so it needs no measurements.
func decodeFeedDetection(object map[string]json.RawMessage, retract bool) (Detection, error) {
	required := []string{"satellite", "acq_date", "acq_time", "latitude", "longitude"}
	if !retract {
		required = append(required, "confidence", "frp")
	}

	for _, key := range required {
		if raw, exists := object[key]; !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return Detection{}, errors.New("missing detection field")
		}
	}

	if raw := object["acq_time"]; len(raw) > 0 && raw[0] != '"' {
		var n int
		if json.Unmarshal(raw, &n) != nil || n < 0 || n > 2359 {
			return Detection{}, errors.New("invalid numeric HHMM")
		}

		object["acq_time"], _ = json.Marshal(fmt.Sprintf("%04d", n))
	}

	raw, _ := json.Marshal(object)

	var d Detection
	if json.Unmarshal(raw, &d) != nil {
		return Detection{}, errors.New("invalid detection types")
	}

	d.Confidence = normalizeConfidence(d.Confidence)
	if retract {
		d.Confidence, d.FRP = "nominal", 0
	}

	if d.Validate() != nil {
		return Detection{}, errors.New("invalid detection values")
	}

	return d, nil
}

func normalizeConfidence(value string) string {
	switch value {
	case "l":
		return "low"
	case "n":
		return "nominal"
	case "h":
		return "high"
	default:
		return value
	}
}

// Feed correction keys encode the same identity as Detection.Key, with pipe
// delimiters and numeric HHMM rather than four-character time strings.
func decodeFeedKey(raw json.RawMessage) (string, error) {
	var value string
	if json.Unmarshal(raw, &value) != nil || value == "" || len(value) > 512 {
		return "", errors.New("invalid correction key")
	}

	if strings.HasPrefix(value, "[") {
		var parts []string
		if json.Unmarshal([]byte(value), &parts) != nil || len(parts) != 5 {
			return "", errors.New("invalid canonical correction key")
		}

		return detectionIdentity(parts)
	}

	return detectionIdentity(strings.Split(value, "|"))
}

func detectionIdentity(parts []string) (string, error) {
	if len(parts) != 5 {
		return "", errors.New("correction key requires five identity fields")
	}

	hhmm, timeErr := strconv.Atoi(parts[2])

	lat, latErr := strconv.ParseFloat(parts[3], 64)
	lon, lonErr := strconv.ParseFloat(parts[4], 64)

	if timeErr != nil || hhmm < 0 || hhmm > 2359 || latErr != nil || lonErr != nil {
		return "", errors.New("invalid correction key identity")
	}

	d := Detection{Satellite: parts[0], AcqDate: parts[1], AcqTime: fmt.Sprintf("%04d", hhmm), Latitude: lat, Longitude: lon, Confidence: "nominal"}
	if err := d.Validate(); err != nil {
		return "", err
	}

	return d.Key(), nil
}

func decodeFeedPatch(raw json.RawMessage) (DetectionPatch, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) == 0 {
		return DetectionPatch{}, errors.New("update changes must be a nonempty object")
	}

	var patch DetectionPatch

	for key, value := range fields {
		switch key {
		case "satellite", "acq_date", "acq_time", "latitude", "longitude":
			return patch, errors.New("partial update changes detection identity")
		case "confidence":
			var confidence string
			if json.Unmarshal(value, &confidence) != nil || confidence == "" {
				return patch, errors.New("invalid updated confidence")
			}

			normalized := normalizeConfidence(confidence)
			if normalized != "low" && normalized != "nominal" && normalized != "high" {
				return patch, errors.New("invalid updated confidence")
			}

			patch.Confidence = &normalized
		case "frp":
			var frp float64
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, &frp) != nil || frp < 0 {
				return patch, errors.New("invalid updated FRP")
			}

			patch.FRP = &frp
		}
	}

	return patch, nil
}
