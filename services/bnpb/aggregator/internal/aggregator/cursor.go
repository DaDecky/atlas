package aggregator

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

type pageCursor struct {
	Time   time.Time `json:"t"`
	ID     string    `json:"id"`
	Filter string    `json:"f"`
	Audit  bool      `json:"a"`
}

func queryFilter(q Query) string {
	since := ""
	if q.Since != nil {
		since = q.Since.UTC().Format(time.RFC3339Nano)
	}

	raw, _ := json.Marshal([]string{q.Source, q.HazardType, q.Severity, since})

	return string(raw)
}

func decodeCursor(q Query) (pageCursor, error) {
	var c pageCursor
	if q.Cursor == "" {
		return c, nil
	}

	if len(q.Cursor) > 4096 {
		return c, errors.New("invalid cursor")
	}

	raw, err := base64.RawURLEncoding.DecodeString(q.Cursor)
	if err != nil {
		return c, errors.New("invalid cursor")
	}

	if err = json.Unmarshal(raw, &c); err != nil || c.ID == "" || c.Time.IsZero() || c.Filter != queryFilter(q) || c.Audit != q.Audit {
		return c, errors.New("cursor does not match query")
	}

	return c, nil
}

func encodeCursor(q Query, h HazardEvent) string {
	when := h.OccurredAt
	if q.Audit {
		when = h.IngestedAt
	}

	raw, _ := json.Marshal(pageCursor{Time: when, ID: h.HazardID, Filter: queryFilter(q), Audit: q.Audit})

	return base64.RawURLEncoding.EncodeToString(raw)
}
