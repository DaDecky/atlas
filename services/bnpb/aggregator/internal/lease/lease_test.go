package lease

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type campaignResult struct {
	term *Term
	err  error
}

// apiServer adds the optimistic concurrency that the fake clientset lacks. After a takeover,
// an update from any other holder or term fails with a conflict, as a stale resourceVersion would.
type apiServer struct {
	client      *fake.Clientset
	mu          sync.Mutex
	pinned      bool
	holder      string
	transitions int32
}

const testNamespace = "atlas"

// The renew deadline leaves room for scheduling stalls under the race detector.
var testTiming = Timing{Duration: 2 * time.Second, RenewDeadline: 1500 * time.Millisecond, RetryPeriod: 200 * time.Millisecond}

func newAPIServer() *apiServer {
	s := &apiServer{client: fake.NewClientset()}
	s.client.PrependReactor("update", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		update := action.(k8stesting.UpdateAction)
		lease := update.GetObject().(*coordinationv1.Lease)

		if s.pinned && (*lease.Spec.HolderIdentity != s.holder || *lease.Spec.LeaseTransitions != s.transitions) {
			return true, nil, apierrors.NewConflict(coordinationv1.Resource("leases"), lease.Name, errors.New("stale holder"))
		}

		return true, lease, s.client.Tracker().Update(update.GetResource(), lease, update.GetNamespace())
	})

	return s
}

func (s *apiServer) elector(t *testing.T) *Elector {
	t.Helper()

	e, err := newElector(s.client, testNamespace, testTiming, slog.New(slog.NewTextHandler(t.Output(), nil)))
	if err != nil {
		t.Fatal(err)
	}

	return e
}

func (s *apiServer) takeOver(t *testing.T, holder string, transitions int32) {
	t.Helper()

	leases := s.client.CoordinationV1().Leases(testNamespace)

	lease, err := leases.Get(context.Background(), leaseName("BMKG"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	s.mu.Lock()
	s.pinned, s.holder, s.transitions = true, holder, transitions
	s.mu.Unlock()

	// Cleanups run in reverse, so the old holder releases against an open server.
	t.Cleanup(func() {
		s.mu.Lock()
		s.pinned = false
		s.mu.Unlock()
	})

	lease.Spec.HolderIdentity, lease.Spec.LeaseTransitions = &holder, &transitions
	if _, err = leases.Update(context.Background(), lease, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func campaign(ctx context.Context, e *Elector) <-chan campaignResult {
	out := make(chan campaignResult, 1)

	go func() {
		term, err := e.Lead(ctx, "BMKG")
		out <- campaignResult{term, err}
	}()

	return out
}

func lead(t *testing.T, e *Elector) *Term {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	term, err := e.Lead(ctx, "BMKG")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(term.Release)

	return term
}

func TestOneOfTwoElectorsLeads(t *testing.T) {
	s := newAPIServer()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	defer cancel()

	first, second := campaign(ctx, s.elector(t)), campaign(ctx, s.elector(t))

	var (
		winner  campaignResult
		waiting <-chan campaignResult
	)

	select {
	case winner = <-first:
		waiting = second
	case winner = <-second:
		waiting = first
	case <-ctx.Done():
		t.Fatal("no elector led")
	}

	if winner.err != nil {
		t.Fatal(winner.err)
	}

	defer winner.term.Release()

	select {
	case loser := <-waiting:
		t.Fatalf("second elector also returned: %v", loser.err)
	case <-time.After(2 * testTiming.Duration):
	}

	if err := winner.term.Check(ctx); err != nil {
		t.Fatalf("leader check failed: %v", err)
	}

	cancel()

	if loser := <-waiting; !errors.Is(loser.err, context.Canceled) {
		t.Fatalf("waiting elector ended with %v", loser.err)
	}
}

func TestReleaseHandsLeaseToWaitingElector(t *testing.T) {
	s := newAPIServer()
	first := lead(t, s.elector(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	next := campaign(ctx, s.elector(t))

	start := time.Now()

	first.Release()

	result := <-next
	if result.err != nil {
		t.Fatalf("lease was not handed over: %v", result.err)
	}

	defer result.term.Release()

	// Faster than the lease duration means the release handed over, not the expiry.
	if waited := time.Since(start); waited >= testTiming.Duration {
		t.Fatalf("handover took %v, want under %v", waited, testTiming.Duration)
	}

	if result.term.transitions != first.transitions+1 {
		t.Fatalf("transitions = %d, want %d", result.term.transitions, first.transitions+1)
	}
}

func TestCheckFailsAfterTakeover(t *testing.T) {
	s := newAPIServer()
	term := lead(t, s.elector(t))

	if err := term.Check(context.Background()); err != nil {
		t.Fatalf("check before takeover: %v", err)
	}

	s.takeOver(t, "other", term.transitions)

	if err := term.Check(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("check after takeover = %v, want ErrLeaseLost", err)
	}
}

func TestCheckFailsOnLaterTermOfSameHolder(t *testing.T) {
	s := newAPIServer()
	term := lead(t, s.elector(t))

	s.takeOver(t, term.identity, term.transitions+2)

	if err := term.Check(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("check in later term = %v, want ErrLeaseLost", err)
	}
}

func TestBadTimingFailsAtStart(t *testing.T) {
	timing := Timing{Duration: time.Second, RenewDeadline: time.Second, RetryPeriod: 100 * time.Millisecond}

	if _, err := newElector(fake.NewClientset(), testNamespace, timing, slog.Default()); err == nil {
		t.Fatal("renew deadline equal to the duration was accepted")
	}
}

func TestLeaseNameIsValidObjectName(t *testing.T) {
	if got := leaseName("SENSOR_PANTAI"); got != "aggregator-sensor-pantai" {
		t.Fatalf("leaseName = %q", got)
	}
}
