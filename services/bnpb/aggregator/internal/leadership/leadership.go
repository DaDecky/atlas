// Package leadership runs one source's work while this instance holds the source lease
// and a sealed writer epoch, and stops the work as soon as either is lost.
package leadership

import (
	"context"
	"log/slog"
	"time"
)

// Guard returns an error once the term is lost. Work calls it before each write.
type Guard func(context.Context) error

// Work runs for one term with its writer epoch, and returns when ctx ends.
type Work func(context.Context, int64, Guard)

// Leader grants a lease term for one source.
type Leader interface {
	Lead(ctx context.Context, source string) (Term, error)
}

// Term is one held lease. Check fails once another instance holds it.
type Term interface {
	Check(ctx context.Context) error
	Release()
}

// Writer seals a new writer epoch, so writes of an older holder are rejected.
type Writer interface {
	AcquireWriterEpoch(ctx context.Context, source string) (int64, error)
}

type Runner struct {
	Leader Leader
	Writer Writer
	Log    *slog.Logger
	// CheckEvery bounds how long a lost term can keep working without a write.
	CheckEvery, CheckTimeout, BootstrapTimeout, RetryDelay time.Duration
}

// NewRunner uses the timings of the deployed system. The lease outlives two missed checks.
func NewRunner(leader Leader, writer Writer, log *slog.Logger) *Runner {
	return &Runner{
		Leader: leader, Writer: writer, Log: log,
		CheckEvery: 2 * time.Second, CheckTimeout: 2 * time.Second, BootstrapTimeout: 8 * time.Second, RetryDelay: time.Second,
	}
}

// Run keeps taking terms for source until ctx ends.
func (r *Runner) Run(ctx context.Context, source string, work Work) {
	for ctx.Err() == nil {
		if err := r.term(ctx, source, work); err != nil && ctx.Err() == nil {
			r.Log.Warn("source_term_ended", "source", source, "reason", err.Error())
		}

		if !wait(ctx, r.RetryDelay) {
			return
		}
	}
}

func (r *Runner) term(ctx context.Context, source string, work Work) error {
	term, err := r.Leader.Lead(ctx, source)
	if err != nil {
		return err
	}
	defer term.Release()

	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()

	guard := r.guard(lifetime, cancel, term)

	epoch, err := r.bootstrap(lifetime, source, guard)
	if err != nil {
		return err
	}

	r.Log.Info("source_writer_ready", "source", source, "epoch", epoch)

	done := make(chan struct{})
	go func() {
		defer close(done)

		r.watch(lifetime, guard)
	}()

	work(lifetime, epoch, guard)
	cancel()
	<-done

	return nil
}

// guard checks the term within CheckTimeout, and ends the term's lifetime when the check fails.
func (r *Runner) guard(lifetime context.Context, cancel context.CancelFunc, term Term) Guard {
	return func(call context.Context) error {
		if err := lifetime.Err(); err != nil {
			return err
		}

		check, done := context.WithTimeout(call, r.CheckTimeout)
		defer done()

		if err := term.Check(check); err != nil {
			cancel()
			return err
		}

		return nil
	}
}

// bootstrap seals a writer epoch between two lease checks, so a term lost meanwhile writes nothing.
func (r *Runner) bootstrap(lifetime context.Context, source string, guard Guard) (int64, error) {
	ctx, stop := context.WithTimeout(lifetime, r.BootstrapTimeout)
	defer stop()

	if err := guard(ctx); err != nil {
		return 0, err
	}

	epoch, err := r.Writer.AcquireWriterEpoch(ctx, source)
	if err != nil {
		return 0, err
	}

	return epoch, guard(ctx)
}

// watch checks the term on a timer, so work that is idle still stops when the term is lost.
func (r *Runner) watch(lifetime context.Context, guard Guard) {
	ticker := time.NewTicker(r.CheckEvery)
	defer ticker.Stop()

	for {
		select {
		case <-lifetime.Done():
			return
		case <-ticker.C:
			if err := guard(lifetime); err != nil {
				return
			}
		}
	}
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
