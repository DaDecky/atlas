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

var ErrLeadershipLost = errors.New("satellite leadership lost")

func (e *feedStatus) Error() string { return fmt.Sprintf("satellite feed status %d", e.status) }

func (r *activityReader) Read(p []byte) (int, error) {
	r.timer.Reset(r.idle)
	n, err := r.reader.Read(p)
	r.timer.Stop()

	return n, err
}

func (r *Runner) connection(ctx context.Context) error {
	if err := r.Guard(ctx); err != nil {
		return err
	}

	raw, err := r.Sink.Load(ctx)
	if err != nil {
		return err
	}

	engine := New()
	if len(raw) > 0 {
		engine, err = Restore(raw)
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	idle := time.AfterFunc(r.IdleTimeout, cancel)
	defer idle.Stop()

	position, hasPosition := engine.Position()
	target := r.Origin + "/v1/stream?from=" + strconv.FormatInt(position, 10)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return errors.New("invalid satellite request")
	}

	req.Header.Set("Authorization", "Bearer "+r.Token)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", "Atlas-IF4031/1.0")

	if hasPosition {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(position, 10))
	}

	resp, err := r.Client.Do(req)
	if err != nil {
		return errors.New("satellite connection failed or idle timeout")
	}

	idle.Stop()

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		delay := time.Duration(0)
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 && seconds <= 60 {
			delay = time.Duration(seconds) * time.Second
		}

		return &feedStatus{status: resp.StatusCode, delay: delay}
	}

	if !strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		return errors.New("satellite response is not SSE")
	}

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

	flushTicker := time.NewTicker(r.BatchInterval)
	defer flushTicker.Stop()

	renewTicker := time.NewTicker(r.RenewInterval)
	defer renewTicker.Stop()

	changes := map[string]Fire{}
	arrivals := []time.Time{}
	pendingFrames := 0
	flush := func() error {
		if pendingFrames == 0 {
			return nil
		}

		if err := r.Guard(ctx); err != nil {
			return err
		}

		payload, err := engine.Snapshot()
		if err != nil {
			return err
		}

		fires := make([]Fire, 0, len(changes))
		for _, fire := range changes {
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
			latencies := make([]time.Duration, len(arrivals))
			for i, arrival := range arrivals {
				latencies[i] = time.Since(arrival)
			}

			r.OnCommit(CommitInfo{Stats: engine.Stats(), Frames: pendingFrames, Latencies: latencies, Watermark: engine.state.Watermark, Duration: time.Since(begun)})
		}

		changes, arrivals = map[string]Fire{}, nil
		pendingFrames = 0

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-renewTicker.C:
			if err := r.Guard(ctx); err != nil {
				return err
			}
		case <-flushTicker.C:
			if err = flush(); err != nil {
				return err
			}
		case event, ok := <-events:
			if !ok {
				if err = flush(); err != nil {
					return err
				}

				return <-ended
			}

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
				continue
			}

			for _, fire := range fires {
				changes[fire.ID] = fire
			}

			if len(fires) > 0 {
				arrivals = append(arrivals, event.ReceivedAt)
			}

			pendingFrames++
			if pendingFrames >= r.BatchSize {
				if err = flush(); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Runner) Run(ctx context.Context) error {
	u, err := url.Parse(r.Origin)
	if err != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Scheme != "http" && u.Scheme != "https" || r.Token == "" || r.Sink == nil || r.Guard == nil {
		return errors.New("invalid satellite configuration or missing leadership guard")
	}

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

	for attempt := 0; ctx.Err() == nil; attempt++ {
		r.committed = false

		err = r.connection(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if errors.Is(err, ErrLeadershipLost) {
			return err
		}

		var status *feedStatus
		if errors.As(err, &status) && status.status >= 400 && status.status < 500 && status.status != 429 {
			return err
		}

		if r.OnError != nil {
			r.OnError(err)
		}

		if r.committed {
			attempt = 0
		}

		delay := time.Duration(1<<min(attempt, 4)) * time.Second

		delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		if r.Backoff != nil {
			delay = r.Backoff(attempt)
		}

		if status != nil && status.delay > delay {
			delay = status.delay
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return ctx.Err()
}
