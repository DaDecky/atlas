package aggregator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"atlas/services/bnpb/aggregator/internal/decode"
)

type IngestStore interface {
	Checkpoint(context.Context, string) (*time.Time, error)
	WarningEvents(context.Context, []string) ([]HazardEvent, error)
	Save(context.Context, string, []HazardEvent, *time.Time, time.Time) ([]HazardEvent, error)
	Failure(context.Context, string, time.Time) error
	Park(context.Context, ParkedMessage) error
}

type Publication struct {
	Event         HazardEvent
	CorrelationID string
}

type Poller struct {
	Source, BaseURL, Credential string
	Interval, Timeout, Backoff  time.Duration
	Retries                     int
	MaxRecords, MaxSourceBytes  int
	WarningLookback             time.Duration
	FailureMarkTimeout          time.Duration
	Client                      *http.Client
	Store                       IngestStore
	Log                         *slog.Logger
	PolicyFile                  string
	Guard                       func(context.Context) error
}

func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *Poller) fetch(ctx context.Context, path string, since *time.Time, id string) ([]sourceRecord, error) {
	for attempt := 0; attempt <= p.Retries; attempt++ {
		if attempt > 0 && !pause(ctx, p.Backoff*time.Duration(1<<(attempt-1))) {
			return nil, ctx.Err()
		}

		if p.Guard != nil {
			if err := p.Guard(ctx); err != nil {
				return nil, err
			}
		}

		records, status, err := p.fetchOnce(ctx, path, since, id)
		if err == nil {
			return records, nil
		}

		if status >= 400 && status < 500 && status != 429 {
			return nil, err
		}

		if status == http.StatusOK {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, errors.New("source attempts exhausted")
}

func (p *Poller) fetchOnce(ctx context.Context, path string, since *time.Time, id string) (
	records []sourceRecord,
	status int,
	err error,
) {
	callCtx, cancel := context.WithTimeout(ctx, p.Timeout)
	defer cancel()

	req, err := p.sourceRequest(callCtx, path, since, id)
	if err != nil {
		return nil, 0, err
	}

	start := time.Now()

	defer func() {
		reason := ""
		if err != nil {
			reason = err.Error()
		}

		p.Log.Info(
			"source_call",
			"source", p.Source,
			"dependency", p.Source,
			"correlation_id", id,
			"method", "GET",
			"path", path,
			"status", status,
			"reason", reason,
			"latency_ms", time.Since(start).Milliseconds(),
		)
	}()

	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, status, errors.New("source network failure")
	}
	defer resp.Body.Close()

	status = resp.StatusCode
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("source status %d", status)
	}

	records, err = p.decodeRecords(ctx, path, id, resp.Body)

	return records, status, err
}

func (p *Poller) sourceRequest(ctx context.Context, path string, since *time.Time, id string) (*http.Request, error) {
	u, err := url.Parse(p.BaseURL + path)
	if err != nil {
		return nil, errors.New("invalid source URL")
	}

	if since != nil {
		q := u.Query()
		q.Set("since", since.UTC().Format(time.RFC3339Nano))
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, errors.New("invalid request")
	}

	req.Header.Set("X-Correlation-ID", id)
	pollerSources[p.Source].authorize(req, p.Credential)

	return req, nil
}

// decodeRecords streams the body and parks each record or body that breaks the boundary rules.
func (p *Poller) decodeRecords(ctx context.Context, path, id string, body io.Reader) ([]sourceRecord, error) {
	var records []sourceRecord

	limits := decode.Limits{BodyBytes: min(cmp.Or(p.MaxSourceBytes, defaultMaxSourceBytes), defaultMaxSourceBytes), Records: cmp.Or(p.MaxRecords, defaultMaxSourceRecords)}

	err := decode.Array(body, limits, func(record decode.Record) error {
		if record.Code != "" {
			p.reject(id, "", record.Code)
			return p.park(ctx, path, id, record.Raw, record.Code, "record failed boundary validation", nil)
		}

		records = append(records, sourceRecord{Object: Object(record.Object), Raw: record.Raw})

		return nil
	})
	if err == nil {
		return records, nil
	}

	var issue *decode.BodyError
	if !errors.As(err, &issue) {
		return nil, err
	}

	if err = p.park(ctx, path, id, issue.Raw, issue.Code, "source body failed boundary validation", issue); err != nil {
		return nil, err
	}

	return records, nil
}

func (p *Poller) reject(id, ref, reason string) {
	p.Log.Warn("record_rejected", "source", p.Source, "correlation_id", id, "source_ref_id", ref, "reason", reason)
}

func (p *Poller) fetchWarnings(ctx context.Context, checkpoint *time.Time, id string) ([]Warning, error) {
	var since *time.Time

	if checkpoint != nil {
		t := checkpoint.Add(-cmp.Or(p.WarningLookback, defaultWarningLookback))
		since = &t
	}

	raw, err := p.fetch(ctx, "/tsunami-warnings", since, id)
	if err != nil {
		return nil, err
	}

	warnings := []Warning{}
	seen := map[string]bool{}

	for _, record := range raw {
		o := record.Object

		ref, w, err := mapWarning(o)
		if err == nil && seen[ref] {
			err = errors.New("duplicate warning ID")
		}

		if err != nil {
			p.reject(id, ref, err.Error())

			if !seen[ref] {
				if parkErr := p.park(ctx, "/tsunami-warnings", id, record.Raw, "schema_violation", err.Error(), nil); parkErr != nil {
					return nil, parkErr
				}
			}

			continue
		}

		seen[ref] = true

		warnings = append(warnings, w)
	}

	return warnings, nil
}

func (p *Poller) Poll(ctx context.Context, id string) error {
	src := pollerSources[p.Source]

	policy, err := p.schemaPolicy(src)
	if err != nil {
		return err
	}

	start := time.Now()
	checkpoint, err := p.Store.Checkpoint(ctx, p.Source)
	logDatabaseCall(p.Log, id, "checkpoint", start, err)

	if err != nil {
		return errors.New("checkpoint read failed")
	}

	raw, err := p.fetch(ctx, src.path, checkpoint, id)
	if err != nil {
		return err
	}

	warnings := []Warning{}
	if src.warnings {
		if warnings, err = p.fetchWarnings(ctx, checkpoint, id); err != nil {
			return err
		}
	}

	events, seen, checkpoint, err := p.mapRecords(ctx, id, src, raw, warnings, policy, checkpoint)
	if err != nil {
		return err
	}

	revised, err := p.revisedByWarnings(ctx, id, warnings, seen)
	if err != nil {
		return err
	}

	return p.save(ctx, id, append(events, revised...), checkpoint)
}

// mapRecords maps each record and parks the ones that fail. Rejected records still
// advance the checkpoint, so one bad record cannot stall the source.
func (p *Poller) mapRecords(ctx context.Context, id string, src pollerSource, raw []sourceRecord, warnings []Warning, policy SchemaPolicy, checkpoint *time.Time) (
	[]HazardEvent, map[string]bool, *time.Time, error,
) {
	now := time.Now().UTC()
	events := []HazardEvent{}
	seen := map[string]bool{}

	for _, record := range raw {
		o := record.Object
		if t, err := timestamp(o, src.timeField); err == nil && (checkpoint == nil || t.After(*checkpoint)) {
			checkpoint = &t
		}

		h, err := src.mapRecord(o, warnings, policy, now)
		if err == nil && seen[h.SourceRefID] {
			p.reject(id, h.SourceRefID, "duplicate source ID")
			continue
		}

		if err != nil {
			ref, _ := textField(o, src.idField)
			p.reject(id, ref, err.Error())

			if parkErr := p.park(ctx, src.path, id, record.Raw, "schema_violation", err.Error(), nil); parkErr != nil {
				return nil, nil, nil, parkErr
			}

			continue
		}

		seen[h.SourceRefID] = true
		events = append(events, h)
	}

	return events, seen, checkpoint, nil
}

// revisedByWarnings applies warnings to stored events that this poll did not fetch again.
func (p *Poller) revisedByWarnings(ctx context.Context, id string, warnings []Warning, seen map[string]bool) ([]HazardEvent, error) {
	refs := []string{}

	for _, w := range warnings {
		if !seen[w.RelatedID] {
			refs = append(refs, w.RelatedID)
		}
	}

	if len(refs) == 0 {
		return nil, nil
	}

	start := time.Now()
	old, err := p.Store.WarningEvents(ctx, refs)
	logDatabaseCall(p.Log, id, "warning_events", start, err)

	if err != nil {
		return nil, errors.New("warning lookup failed")
	}

	revised := make([]HazardEvent, 0, len(old))
	for _, h := range old {
		revised = append(revised, ApplyWarnings(h, warnings))
	}

	return revised, nil
}

func (p *Poller) save(ctx context.Context, id string, events []HazardEvent, checkpoint *time.Time) error {
	if p.Guard != nil {
		if err := p.Guard(ctx); err != nil {
			return err
		}
	}

	start := time.Now()
	changed, err := p.Store.Save(ctx, p.Source, events, checkpoint, time.Now().UTC())
	logDatabaseCall(p.Log, id, "save", start, err)

	if err != nil {
		return errors.New("canonical commit failed")
	}

	p.Log.Info(
		"poll_committed",
		"source", p.Source,
		"correlation_id", id,
		"published_revisions", len(changed),
		"mapped", len(events),
		"checkpoint", checkpoint,
	)

	return nil
}

// Two fetches, events and warnings, each with every retry and its backoff, plus slack for the database.
func (p *Poller) pollBudget() time.Duration {
	fetch := p.Timeout*time.Duration(p.Retries+1) + p.Backoff*time.Duration(1<<p.Retries)

	return 2*fetch + 5*time.Second
}

func (p *Poller) Run(ctx context.Context) {
	for ctx.Err() == nil {
		id := correlationID()
		pollCtx, cancel := context.WithTimeout(ctx, p.pollBudget())
		err := p.Poll(pollCtx, id)

		cancel()

		if err != nil && ctx.Err() == nil {
			failureCtx, stop := context.WithTimeout(ctx, cmp.Or(p.FailureMarkTimeout, defaultFailureMarkTimeout))
			start := time.Now()
			markErr := p.Store.Failure(failureCtx, p.Source, time.Now().UTC())
			logDatabaseCall(p.Log, id, "failure", start, markErr)
			stop()
			p.Log.Warn(
				"poll_failed",
				"source", p.Source,
				"correlation_id", id,
				"reason", err.Error(),
				"freshness_saved", markErr == nil,
			)
		}

		if !pause(ctx, p.Interval) {
			return
		}
	}
}
