package aggregator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/leadership"
)

// plannerTerm holds for the first validFor checks, as a lease that another pod later takes.
type plannerTerm struct {
	checks   atomic.Int32
	validFor int32
	released atomic.Bool
}

type plannerLeader struct {
	leads  atomic.Int32
	terms  []*plannerTerm
	cancel context.CancelFunc
}

func (t *plannerTerm) Check(context.Context) error {
	if t.checks.Add(1) > t.validFor {
		return errors.New("lease held by another planner")
	}

	return nil
}

func (t *plannerTerm) Release() { t.released.Store(true) }

func (l *plannerLeader) Lead(context.Context, string) (leadership.Term, error) {
	n := int(l.leads.Add(1))
	if n > len(l.terms) {
		l.cancel()

		return nil, context.Canceled
	}

	return l.terms[n-1], nil
}

func TestPlannerReleasesLostTermAndLeadsAgain(t *testing.T) {
	c, _ := racingCluster(t)
	term := &plannerTerm{validFor: 1}
	ctx, cancel := context.WithCancel(context.Background())
	leader := &plannerLeader{terms: []*plannerTerm{term}, cancel: cancel}

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.RunRecovery(ctx, leader, func(err error) { t.Error(err) })
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("planner kept running after its term was lost")
	}

	if !term.released.Load() || leader.leads.Load() != 2 {
		t.Fatalf("released %v, leads %d", term.released.Load(), leader.leads.Load())
	}
}
