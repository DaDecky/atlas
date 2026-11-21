package aggregator

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"atlas/services/bnpb/aggregator/internal/placement"
)

type AuditData interface {
	ReadStore
	Checksum(context.Context, string) (Checksum, error)
	Location(context.Context, string) (LocationResult, error)
}

type Checksum struct {
	Source           string    `json:"source"`
	Count            int64     `json:"count"`
	Checksum         string    `json:"checksum"`
	Algorithm        string    `json:"algorithm"`
	AsOf             time.Time `json:"as_of"`
	Partial          bool      `json:"partial"`
	UnavailableUnits []string  `json:"unavailable_units"`
}

type Location struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

type LocationResult struct {
	HazardID  string     `json:"hazard_id"`
	Locations []Location `json:"locations"`
	Partial   bool       `json:"partial"`
}

type AuditAPI struct {
	Store   AuditData
	Token   string
	Timeout time.Duration
	Parking Parking
}

var ErrNotFound = errors.New("not found")

func (a *AuditAPI) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/audit/events", a.events)
	mux.HandleFunc("GET /internal/audit/checksum", a.checksum)
	mux.HandleFunc("GET /internal/audit/location", a.location)
	mux.HandleFunc("GET /internal/audit/unprocessed", a.unprocessed)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if a.Token == "" || !ok || subtle.ConstantTimeCompare([]byte(token), []byte(a.Token)) != 1 {
			auditError(w, 401, "unauthorized", "token tidak valid")
			return
		}

		d := a.Timeout
		if d <= 0 {
			d = 5 * time.Second
		}

		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()

		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func auditError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func auditQuery(r *http.Request, allowed ...string) (Query, error) {
	q := Query{Limit: 100, Audit: true}

	keys := map[string]bool{}
	for _, key := range allowed {
		keys[key] = true
	}

	for key, values := range r.URL.Query() {
		if !keys[key] || len(values) != 1 {
			return q, errors.New("invalid parameter")
		}

		v := values[0]

		switch key {
		case "source":
			if !validSource(v) {
				return q, errors.New("invalid source")
			}

			q.Source = v
		case "limit":
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 1000 {
				return q, errors.New("invalid limit")
			}

			q.Limit = n
		case "cursor":
			q.Cursor = v
		case "since":
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return q, err
			}

			q.Since = &t
		}
	}

	_, err := decodeCursor(q)

	return q, err
}

func (a *AuditAPI) events(w http.ResponseWriter, r *http.Request) {
	q, err := auditQuery(r, "source", "since", "limit", "cursor")
	if err != nil {
		auditError(w, 400, "bad_request", err.Error())
		return
	}

	page, err := a.Store.Read(r.Context(), q)
	if err != nil {
		auditError(w, 503, "unavailable", "storage unavailable")
		return
	}

	writeJSON(w, 200, map[string]any{"items": page.Data, "next_cursor": page.NextCursor, "partial": page.Partial, "unavailable_units": page.UnavailableUnits})
}

func (a *AuditAPI) checksum(w http.ResponseWriter, r *http.Request) {
	q, err := auditQuery(r, "source")
	if err != nil {
		auditError(w, 400, "bad_request", err.Error())
		return
	}

	result, err := a.Store.Checksum(r.Context(), q.Source)
	if err != nil {
		auditError(w, 503, "unavailable", "checksum unavailable")
		return
	}

	writeJSON(w, 200, result)
}

func (a *AuditAPI) location(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()) != 1 || len(r.URL.Query()["hazard_id"]) != 1 || r.URL.Query().Get("hazard_id") == "" {
		auditError(w, 400, "bad_request", "hazard_id required")
		return
	}

	result, err := a.Store.Location(r.Context(), r.URL.Query().Get("hazard_id"))
	if errors.Is(err, ErrNotFound) {
		auditError(w, 404, "not_found", "hazard not found")
		return
	}

	if err != nil {
		auditError(w, 503, "unavailable", "location unavailable")
		return
	}

	writeJSON(w, 200, result)
}

func (s *Store) references(ctx context.Context, source, after string, first bool) ([]string, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `SELECT source_ref_id FROM hazard_events WHERE ($1='' OR source=$1) AND ($3 OR source_ref_id COLLATE "C">$2) ORDER BY source_ref_id COLLATE "C",source LIMIT 1000`, source, after, first)
	if err != nil {
		return nil, err
	}

	refs := []string{}

	for rows.Next() {
		var ref string
		if err = rows.Scan(&ref); err != nil {
			rows.Close()
			return nil, err
		}

		refs = append(refs, ref)
	}

	err = rows.Err()
	rows.Close()

	if err != nil {
		return nil, err
	}

	return refs, tx.Commit(ctx)
}

// checksumStores merges byte-sorted shard streams with bounded buffers. The
// no-source case merges separate source streams so equal references across
// sources are retained as separate records.
func checksumStores(ctx context.Context, source string, stores map[string]*Store) (Checksum, error) {
	result := Checksum{Source: source, Algorithm: "sha256", AsOf: time.Now().UTC(), UnavailableUnits: []string{}}

	type stream struct {
		key    string
		source string
		s      *Store
		refs   []string
		at     int
		done   bool
	}

	streams := []*stream{}

	requested := []string{source}
	if source == "" {
		requested = Sources
	}

	for key, s := range stores {
		for _, src := range requested {
			refs, err := s.references(ctx, src, "", true)
			if err != nil {
				result.Partial = true
				result.UnavailableUnits = append(result.UnavailableUnits, key)

				continue
			}

			streams = append(streams, &stream{key: key, source: src, s: s, refs: refs, done: len(refs) < 1000})
		}
	}

	hash := sha256.New()

	for {
		var best *stream
		for _, s := range streams {
			if s.at < len(s.refs) && (best == nil || s.refs[s.at] < best.refs[best.at]) {
				best = s
			}
		}

		if best == nil {
			break
		}

		ref := best.refs[best.at]

		if result.Count > 0 {
			_, _ = hash.Write([]byte{'\n'})
		}

		_, _ = hash.Write([]byte(ref))
		result.Count++

		best.at++
		if best.at == len(best.refs) && !best.done {
			refs, err := best.s.references(ctx, best.source, ref, false)
			if err != nil {
				result.Partial = true
				result.UnavailableUnits = append(result.UnavailableUnits, best.key)
				best.done = true

				continue
			}

			best.refs = refs
			best.at = 0
			best.done = len(refs) < 1000
		}
	}

	result.Checksum = hex.EncodeToString(hash.Sum(nil))
	sort.Strings(result.UnavailableUnits)

	return result, nil
}

func (s *Store) Checksum(ctx context.Context, source string) (Checksum, error) {
	return checksumStores(ctx, source, map[string]*Store{"single": s})
}

func (c *Cluster) Checksum(ctx context.Context, source string) (Checksum, error) {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return Checksum{}, err
	}

	stores := map[string]*Store{}
	missing := []string{}

	for shard, copies := range t.Shards {
		found := false

		for _, holder := range copies {
			if holder.State != placement.Serving || c.Nodes[holder.Node] == nil {
				continue
			}

			s := c.copyStore(holder, shard)
			call, cancel := c.timeout(ctx)
			_, err := s.references(call, source, "", true)

			cancel()

			if err == nil {
				stores[shard] = s
				found = true

				break
			}
		}

		if !found {
			missing = append(missing, shard)
		}
	}

	result, err := checksumStores(ctx, source, stores)
	result.UnavailableUnits = append(result.UnavailableUnits, missing...)
	sort.Strings(result.UnavailableUnits)
	result.Partial = len(result.UnavailableUnits) > 0

	return result, err
}

func (s *Store) contains(ctx context.Context, id string) (bool, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var exists bool

	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM hazard_events WHERE hazard_id=$1)`, id).Scan(&exists)

	return exists, err
}

func (s *Store) Location(ctx context.Context, id string) (LocationResult, error) {
	exists, err := s.contains(ctx, id)

	r := LocationResult{HazardID: id, Locations: []Location{}}
	if err != nil {
		return r, err
	}

	if !exists {
		return r, ErrNotFound
	}

	r.Locations = append(r.Locations, Location{Node: "single", Unit: "single", Role: "primary"})

	return r, nil
}

func (c *Cluster) Location(ctx context.Context, id string) (LocationResult, error) {
	r := LocationResult{HazardID: id, Locations: []Location{}}

	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return r, err
	}

	shard := placement.Shard(id)
	for i, holder := range t.Shards[shard] {
		if holder.State != placement.Serving || c.Nodes[holder.Node] == nil {
			r.Partial = true
			continue
		}

		call, cancel := c.timeout(ctx)
		exists, err := c.copyStore(holder, shard).contains(call, id)

		cancel()

		if err != nil {
			r.Partial = true
			continue
		}

		if exists {
			role, unit := "primary", shard
			if i > 0 {
				role, unit = "replica", shard+"-r1"
			}

			r.Locations = append(r.Locations, Location{Node: holder.Node, Unit: unit, Role: role})
		}
	}

	if len(r.Locations) == 0 && !r.Partial {
		return r, ErrNotFound
	}

	return r, nil
}
