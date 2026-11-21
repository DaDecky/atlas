package leadership

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeTerm struct {
	lost     atomic.Bool
	released atomic.Bool
}

type fakeLeader struct {
	mu    sync.Mutex
	terms []*fakeTerm
}

type fakeWriter struct {
	epoch int64
	err   error
}

func (t *fakeTerm) Check(context.Context) error {
	if t.lost.Load() {
		return errors.New("lease held by another instance")
	}

	return nil
}

func (t *fakeTerm) Release() { t.released.Store(true) }

func (l *fakeLeader) Lead(context.Context, string) (Term, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	term := &fakeTerm{}
	l.terms = append(l.terms, term)

	return term, nil
}

func (l *fakeLeader) taken() []*fakeTerm {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]*fakeTerm(nil), l.terms...)
}

func (w fakeWriter) AcquireWriterEpoch(context.Context, string) (int64, error) {
	return w.epoch, w.err
}

func testRunner(leader Leader, writer Writer) *Runner {
	return &Runner{
		Leader: leader, Writer: writer, Log: slog.New(slog.DiscardHandler),
		CheckEvery: 10 * time.Millisecond, CheckTimeout: 50 * time.Millisecond,
		BootstrapTimeout: 100 * time.Millisecond, RetryDelay: 10 * time.Millisecond,
	}
}

func TestWorkGetsEpochAndPassingGuard(t *testing.T) {
	leader := &fakeLeader{}

	var (
		gotEpoch int64
		guardErr error
	)

	err := testRunner(leader, fakeWriter{epoch: 7}).term(context.Background(), "BMKG", func(ctx context.Context, epoch int64, guard Guard) {
		gotEpoch, guardErr = epoch, guard(ctx)
	})
	if err != nil || gotEpoch != 7 || guardErr != nil {
		t.Fatalf("term error %v, epoch %d, guard %v", err, gotEpoch, guardErr)
	}

	if !leader.taken()[0].released.Load() {
		t.Fatal("term not released after work")
	}
}

func TestLostTermCancelsWork(t *testing.T) {
	leader := &fakeLeader{}
	stopped := make(chan struct{})

	go func() {
		_ = testRunner(leader, fakeWriter{epoch: 1}).term(context.Background(), "BMKG", func(ctx context.Context, _ int64, _ Guard) {
			<-ctx.Done()
			close(stopped)
		})
	}()

	for len(leader.taken()) == 0 {
		time.Sleep(time.Millisecond)
	}

	leader.taken()[0].lost.Store(true)

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("idle work kept running after the term was lost")
	}
}

func TestEpochFailureReleasesTermWithoutWork(t *testing.T) {
	leader := &fakeLeader{}
	ran := false

	err := testRunner(leader, fakeWriter{err: errors.New("seal majority unavailable")}).term(context.Background(), "BMKG", func(context.Context, int64, Guard) {
		ran = true
	})
	if err == nil || ran {
		t.Fatalf("term error %v, work ran %v", err, ran)
	}

	if !leader.taken()[0].released.Load() {
		t.Fatal("term not released after failed epoch")
	}
}

func TestRunTakesNewTermAfterWorkEnds(t *testing.T) {
	leader := &fakeLeader{}
	ctx, cancel := context.WithCancel(context.Background())

	var runs atomic.Int32

	done := make(chan struct{})

	go func() {
		defer close(done)

		testRunner(leader, fakeWriter{epoch: 1}).Run(ctx, "BMKG", func(context.Context, int64, Guard) {
			if runs.Add(1) == 2 {
				cancel()
			}
		})
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not take a second term")
	}

	if len(leader.taken()) != 2 {
		t.Fatalf("terms taken %d, want 2", len(leader.taken()))
	}
}
