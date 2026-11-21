package satellite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Sink interface {
	Load(context.Context) ([]byte, error)
	Commit(context.Context, int64, []byte, []Fire) error
}

type CommitInfo struct {
	Stats     Stats
	Frames    int
	Latencies []time.Duration
	Watermark time.Time
	Duration  time.Duration
}

type Runner struct {
	Origin, Token string
	Client        *http.Client
	Sink          Sink
	Guard         func(context.Context) error
	Decode        func(SSEEvent) (Frame, error)
	IdleTimeout   time.Duration
	BatchInterval time.Duration
	RenewInterval time.Duration
	BatchSize     int
	Backoff       func(int) time.Duration
	OnCommit      func(CommitInfo)
	OnError       func(error)
	committed     bool
}

type feedStatus struct {
	status int
	delay  time.Duration
}

type activityReader struct {
	reader io.Reader
	timer  *time.Timer
	idle   time.Duration
}

// batch holds the fires changed since the last commit and when their frames arrived.
type batch struct {
	changes  map[string]Fire
	arrivals []time.Time
	frames   int
}

var ErrLeadershipLost = errors.New("satellite leadership lost")

func (e *feedStatus) Error() string { return fmt.Sprintf("satellite feed status %d", e.status) }

func (r *activityReader) Read(p []byte) (int, error) {
	r.timer.Reset(r.idle)
	n, err := r.reader.Read(p)
	r.timer.Stop()

	return n, err
}

func (r *Runner) connection(ctx context.Context) error {
	engine, err := r.restore(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	idle := time.AfterFunc(r.IdleTimeout, cancel)
	defer idle.Stop()

	resp, err := r.open(ctx, engine)
	if err != nil {
		return err
	}

	idle.Stop()

	defer resp.Body.Close()

	events := make(chan SSEEvent, 32)
	ended := make(chan error, 1)

	go func() {
		defer close(events)

		ended <- ReadSSE(ctx, &activityReader{reader: resp.Body, timer: idle, idle: r.IdleTimeout}, func(event SSEEvent) error {
			select {
			case events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()

	return r.consume(ctx, engine, events, ended)
}

// restore rebuilds the engine from the committed state, under a valid term.
func (r *Runner) restore(ctx context.Context) (*Engine, error) {
	if err := r.Guard(ctx); err != nil {
		return nil, err
	}

	raw, err := r.Sink.Load(ctx)
	if err != nil {
		return nil, err
	}

	if len(raw) == 0 {
		return New(), nil
	}

	return Restore(raw)
}

// open requests the stream from the committed offset and accepts only an SSE answer.
func (r *Runner) open(ctx context.Context, engine *Engine) (*http.Response, error) {
	position, hasPosition := engine.Position()
	target := r.Origin + "/v1/stream?from=" + strconv.FormatInt(position, 10)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, errors.New("invalid satellite request")
	}

	req.Header.Set("Authorization", "Bearer "+r.Token)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", "Atlas-IF4031/1.0")

	if hasPosition {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(position, 10))
	}

	resp, err := r.Client.Do(req)
	if err != nil {
		return nil, errors.New("satellite connection failed or idle timeout")
	}

	if resp.StatusCode != 200 {
		resp.Body.Close()

		delay := time.Duration(0)
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 && seconds <= 60 {
			delay = time.Duration(seconds) * time.Second
		}

		return nil, &feedStatus{status: resp.StatusCode, delay: delay}
	}

	if !strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		resp.Body.Close()

		return nil, errors.New("satellite response is not SSE")
	}

	return resp, nil
}

// consume applies frames as they arrive, commits in batches, and renews the term on a timer.
func (r *Runner) consume(ctx context.Context, engine *Engine, events <-chan SSEEvent, ended <-chan error) error {
	flushTicker := time.NewTicker(r.BatchInterval)
	defer flushTicker.Stop()

	renewTicker := time.NewTicker(r.RenewInterval)
	defer renewTicker.Stop()

	pending := &batch{changes: map[string]Fire{}}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-renewTicker.C:
			if err := r.Guard(ctx); err != nil {
				return err
			}
		case <-flushTicker.C:
			if err := r.flush(ctx, engine, pending); err != nil {
				return err
			}
		case event, ok := <-events:
			if !ok {
				if err := r.flush(ctx, engine, pending); err != nil {
					return err
				}

				return <-ended
			}

			if err := r.apply(ctx, engine, pending, event); err != nil {
				return err
			}
		}
	}
}

// apply adds one frame to the batch. A frame at or before the committed offset changes nothing.
func (r *Runner) apply(ctx context.Context, engine *Engine, pending *batch, event SSEEvent) error {
	frame, err := r.Decode(event)
	if err != nil {
		return err
	}

	before, hasBefore := engine.Position()

	fires, err := engine.Apply(frame)
	if err != nil {
		return err
	}

	if hasBefore && frame.Offset <= before {
		return nil
	}

	for _, fire := range fires {
		pending.changes[fire.ID] = fire
	}

	if len(fires) > 0 {
		pending.arrivals = append(pending.arrivals, event.ReceivedAt)
	}

	pending.frames++
	if pending.frames >= r.BatchSize {
		return r.flush(ctx, engine, pending)
	}

	return nil
}

// flush commits the engine state, the offset, and the changed fires together.
func (r *Runner) flush(ctx context.Context, engine *Engine, pending *batch) error {
	if pending.frames == 0 {
		return nil
	}

	if err := r.Guard(ctx); err != nil {
		return err
	}

	payload, err := engine.Snapshot()
	if err != nil {
		return err
	}

	fires := make([]Fire, 0, len(pending.changes))
	for _, fire := range pending.changes {
		fires = append(fires, fire)
	}

	sort.Slice(fires, func(i, j int) bool { return fires[i].ID < fires[j].ID })

	begun := time.Now()

	position, _ := engine.Position()
	if err = r.Sink.Commit(ctx, position, payload, fires); err != nil {
		return err
	}

	r.committed = true
	if r.OnCommit != nil {
		latencies := make([]time.Duration, len(pending.arrivals))
		for i, arrival := range pending.arrivals {
			latencies[i] = time.Since(arrival)
		}

		r.OnCommit(CommitInfo{Stats: engine.Stats(), Frames: pending.frames, Latencies: latencies, Watermark: engine.state.Watermark, Duration: time.Since(begun)})
	}

	*pending = batch{changes: map[string]Fire{}}

	return nil
}

func (r *Runner) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	r.applyDefaults()

	for attempt := 0; ctx.Err() == nil; attempt++ {
		r.committed = false

		err := r.connection(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var status *feedStatus
		if errors.Is(err, ErrLeadershipLost) || errors.As(err, &status) && status.status >= 400 && status.status < 500 && status.status != 429 {
			return err
		}

		if r.OnError != nil {
			r.OnError(err)
		}

		if r.committed {
			attempt = 0
		}

		timer := time.NewTimer(r.retryDelay(attempt, status))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return ctx.Err()
}

func (r *Runner) validate() error {
	u, err := url.Parse(r.Origin)
	if err != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Scheme != "http" && u.Scheme != "https" || r.Token == "" || r.Sink == nil || r.Guard == nil {
		return errors.New("invalid satellite configuration or missing leadership guard")
	}

	return nil
}

func (r *Runner) applyDefaults() {
	if r.Client == nil {
		r.Client = &http.Client{Timeout: 0, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}

	if r.Decode == nil {
		r.Decode = DecodeFeed
	}

	if r.IdleTimeout <= 0 {
		r.IdleTimeout = 30 * time.Second
	}

	if r.BatchInterval <= 0 {
		r.BatchInterval = 100 * time.Millisecond
	}

	if r.RenewInterval <= 0 {
		r.RenewInterval = 2 * time.Second
	}

	if r.BatchSize <= 0 {
		r.BatchSize = 64
	}
}

// retryDelay grows from 1 s to 16 s with jitter, and honors a longer Retry-After of the feed.
func (r *Runner) retryDelay(attempt int, status *feedStatus) time.Duration {
	delay := time.Duration(1<<min(attempt, 4)) * time.Second

	delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
	if r.Backoff != nil {
		delay = r.Backoff(attempt)
	}

	if status != nil && status.delay > delay {
		delay = status.delay
	}

	return delay
}
