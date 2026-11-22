// Package lease elects one leader per source through Kubernetes Leases.
// No client-go type leaves this package.
package lease

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Timing holds the election periods. A follower takes over after Duration without a renewal.
type Timing struct {
	Duration, RenewDeadline, RetryPeriod time.Duration
}

// Elector campaigns for one Lease per source.
type Elector struct {
	client    kubernetes.Interface
	namespace string
	identity  string
	timing    Timing
	log       *slog.Logger
}

// Term is one leadership of one source. It ends at Release or when the Lease is lost.
type Term struct {
	source      string
	identity    string
	name        string
	leases      coordinationv1.LeaseInterface
	transitions int32
	stop        context.CancelFunc
	lost        chan struct{}
	done        chan struct{}
	log         *slog.Logger
}

var ErrLeaseLost = errors.New("lease lost")

// NewInCluster builds an Elector from the service account of the pod.
func NewInCluster(namespace string, timing Timing, log *slog.Logger) (*Elector, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}

	// Renewals, source checks, and the 100 ms Satellite batch checks share
	// this limiter when one pod holds several leases. Leave room for all three.
	config.QPS = 30
	config.Burst = 60

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}

	return newElector(client, namespace, timing, log)
}

func newElector(client kubernetes.Interface, namespace string, timing Timing, log *slog.Logger) (*Elector, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("read host name: %w", err)
	}

	e := &Elector{client: client, namespace: namespace, identity: host + "_" + rand.Text(), timing: timing, log: log}
	probe := &resourcelock.LeaseLock{LockConfig: resourcelock.ResourceLockConfig{Identity: e.identity}}

	// Building one elector applies the timing rules of client-go at start instead of at the first campaign.
	if _, err = leaderelection.NewLeaderElector(e.config(context.Background(), probe, "", nil, nil)); err != nil {
		return nil, fmt.Errorf("lease timing: %w", err)
	}

	return e, nil
}

// Lead blocks until this process holds the Lease of source, or until ctx ends.
func (e *Elector) Lead(ctx context.Context, source string) (*Term, error) {
	leases := e.client.CoordinationV1().Leases(e.namespace)
	t := &Term{
		source:   source,
		identity: e.identity,
		name:     leaseName(source),
		leases:   leases,
		lost:     make(chan struct{}),
		done:     make(chan struct{}),
		log:      e.log,
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Namespace: e.namespace, Name: t.name},
		Client:     e.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: e.identity},
	}
	started := make(chan struct{})

	// The campaign outlives ctx, so a shutdown releases the Lease only after the pollers stop.
	run, stop := context.WithCancel(logr.NewContextWithSlogLogger(context.WithoutCancel(ctx), e.log))
	t.stop = stop

	elector, err := leaderelection.NewLeaderElector(e.config(run, lock, source, started, t.lost))
	if err != nil {
		stop()

		return nil, fmt.Errorf("configure election of %s: %w", source, err)
	}

	go func() {
		defer close(t.done)

		elector.Run(run)
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Release()

		return nil, ctx.Err()
	}

	holder, transitions, err := t.read(ctx)
	if err != nil {
		t.Release()

		return nil, err
	}

	if holder != e.identity {
		t.Release()

		return nil, ErrLeaseLost
	}

	t.transitions = transitions
	e.log.Info("lease_acquired", "source", source, "identity", e.identity, "transitions", transitions)

	return t, nil
}

func (e *Elector) config(
	run context.Context, lock resourcelock.Interface, source string, started, lost chan struct{},
) leaderelection.LeaderElectionConfig {
	return leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   e.timing.Duration,
		RenewDeadline:   e.timing.RenewDeadline,
		RetryPeriod:     e.timing.RetryPeriod,
		ReleaseOnCancel: true,
		Name:            source,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) { close(started) },
			OnStoppedLeading: func() {
				if run.Err() == nil {
					e.log.Warn("lease_lost", "source", source, "identity", e.identity)
				}

				close(lost)
			},
		},
	}
}

// Check reads the Lease from the API server, because the elector keeps its own view only in memory.
// It fails when another process holds the Lease, or when this one holds it in a later term.
func (t *Term) Check(ctx context.Context) error {
	select {
	case <-t.lost:
		return ErrLeaseLost
	default:
	}

	holder, transitions, err := t.read(ctx)
	if err != nil {
		return err
	}

	if holder != t.identity || transitions != t.transitions {
		return ErrLeaseLost
	}

	return nil
}

// Release ends the term and waits until client-go has given up the Lease.
func (t *Term) Release() {
	t.stop()
	<-t.done
	t.log.Info("lease_released", "source", t.source, "identity", t.identity)
}

// read uses its own request, since the lock object of client-go is not safe to share.
func (t *Term) read(ctx context.Context) (string, int32, error) {
	lease, err := t.leases.Get(ctx, t.name, metav1.GetOptions{})
	if err != nil {
		return "", 0, fmt.Errorf("read lease of %s: %w", t.source, err)
	}

	var (
		holder      string
		transitions int32
	)

	if lease.Spec.HolderIdentity != nil {
		holder = *lease.Spec.HolderIdentity
	}

	if lease.Spec.LeaseTransitions != nil {
		transitions = *lease.Spec.LeaseTransitions
	}

	return holder, transitions, nil
}

// leaseName turns a source such as SENSOR_PANTAI into a valid object name.
func leaseName(source string) string {
	return "aggregator-" + strings.ReplaceAll(strings.ToLower(source), "_", "-")
}
