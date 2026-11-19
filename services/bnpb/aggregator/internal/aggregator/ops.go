package aggregator

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ReplaySource func(context.Context, int) (int, error)

type OpsAPI struct {
	Parking Parking
	Token   string
	Replay  map[string]ReplaySource
}

func (a *OpsAPI) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/ops/unprocessed", a.unprocessed)
	mux.HandleFunc("POST /internal/ops/replay", a.replay)

	capacity := make(chan struct{}, 2)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if a.Token == "" || !ok || subtle.ConstantTimeCompare([]byte(token), []byte(a.Token)) != 1 {
			auditError(w, 401, "unauthorized", "token tidak valid")
			return
		}

		select {
		case capacity <- struct{}{}:
			defer func() { <-capacity }()
		default:
			auditError(w, 503, "ops_busy", "coba lagi")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *OpsAPI) unprocessed(w http.ResponseWriter, r *http.Request) {
	q, err := r.URL.Query(), error(nil)
	for key, values := range q {
		if len(values) != 1 || key != "source" && key != "limit" && key != "cursor" && key != "include_raw" {
			err = errors.New("invalid parameter")
		}
	}

	source, limit, includeRaw := q.Get("source"), 10, false
	if source != "" && !validSource(source) {
		err = errors.New("invalid source")
	}

	if raw := q.Get("limit"); raw != "" {
		var parseErr error

		limit, parseErr = strconv.Atoi(raw)
		if parseErr != nil || limit < 1 || limit > 10 {
			err = errors.New("limit must be between 1 and 10")
		}
	}

	if raw := q.Get("include_raw"); raw != "" {
		if raw != "true" && raw != "false" {
			err = errors.New("include_raw must be a boolean")
		}

		includeRaw = raw == "true"
		if includeRaw {
			limit = 1 // A full 4 MiB payload base64-encodes within the 8 MiB response boundary.
		}
	}

	if err != nil || len(q.Get("cursor")) > 4096 {
		auditError(w, 400, "bad_request", "parameter tidak valid")
		return
	}

	page, err := a.Parking.ListUnprocessed(r.Context(), source, q.Get("cursor"), limit)
	if err != nil {
		auditError(w, 503, "parking_unavailable", "pesan gagal tidak tersedia")
		return
	}

	if !includeRaw {
		for i := range page.Items {
			page.Items[i].Raw = nil
		}
	}

	writeJSON(w, 200, page)
}

func (a *OpsAPI) replay(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source string `json:"source"`
		Limit  int    `json:"limit"`
	}

	decoder := json.NewDecoder(io.LimitReader(r.Body, 4097))
	decoder.DisallowUnknownFields()

	var extra any
	if decoder.Decode(&input) != nil || decoder.Decode(&extra) != io.EOF || !validSource(input.Source) || r.URL.RawQuery != "" {
		auditError(w, 400, "bad_request", "source wajib diisi")
		return
	}

	if input.Limit == 0 {
		input.Limit = 100
	}

	if input.Limit < 1 || input.Limit > 1000 {
		auditError(w, 400, "bad_request", "limit tidak valid")
		return
	}

	replay := a.Replay[input.Source]
	if replay == nil {
		auditError(w, 503, "source_not_leader", "replay hanya dijalankan pemegang sumber")
		return
	}

	count, err := replay(r.Context(), input.Limit)
	if err != nil {
		auditError(w, 503, "replay_unavailable", "pemrosesan ulang belum berhasil")
		return
	}

	writeJSON(w, 200, map[string]any{"source": input.Source, "recovered": count})
}
