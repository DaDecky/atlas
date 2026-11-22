package aggregator

import (
	"errors"
	"net/http"
)

func (a *AuditAPI) unprocessed(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()["cursor"]) > 1 {
		auditError(w, 400, "bad_request", "repeated cursor")
		return
	}

	copyRequest := r.Clone(r.Context())
	copyURL := *r.URL
	copyRequest.URL = &copyURL
	params := r.URL.Query()
	params.Del("cursor")
	copyRequest.URL.RawQuery = params.Encode()

	q, err := auditQuery(copyRequest, "source", "limit")
	if err != nil {
		auditError(w, 400, "bad_request", err.Error())
		return
	}

	cursor := r.URL.Query().Get("cursor")

	if a.Parking == nil {
		auditError(w, 503, "unavailable", "parking unavailable")
		return
	}

	page, err := a.Parking.ListUnprocessed(r.Context(), q.Source, cursor, q.Limit)
	if err != nil {
		if errors.Is(err, errInvalidParkingCursor) {
			auditError(w, 400, "bad_request", err.Error())
		} else {
			auditError(w, 503, "unavailable", "parking unavailable")
		}

		return
	}

	items := []map[string]any{}
	for _, m := range page.Items {
		items = append(items, map[string]any{"message_id": m.MessageID, "source": m.Source, "received_at": m.ReceivedAt, "reason": m.Reason, "retryable": m.Retryable, "attempts": m.Attempts, "status": m.Status})
	}

	result := map[string]any{"items": items, "next_cursor": page.NextCursor}
	if page.Partial {
		result["partial"] = true
		result["unavailable_units"] = page.UnavailableUnits
	}

	writeJSON(w, 200, result)
}
