package aggregator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestHazardReadCacheSharesConcurrentReadsAndExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			cache hazardReadCache
			calls atomic.Int32
		)

		started, release := make(chan struct{}), make(chan struct{})
		read := func(context.Context, Query) (Response, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}

			return testHazardPage(), nil
		}

		ctx := context.Background()
		q := Query{Source: SourceBMKG, Limit: 20}
		errs := make(chan error, 2)

		go func() {
			_, err := cache.read(ctx, q, time.Second, read)
			errs <- err
		}()

		<-started

		go func() {
			_, err := cache.read(ctx, q, time.Second, read)
			errs <- err
		}()

		synctest.Wait()
		close(release)

		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}

		if _, err := cache.read(ctx, q, time.Second, read); err != nil || calls.Load() != 1 {
			t.Fatalf("repeated reads used %d store calls, error %v", calls.Load(), err)
		}

		other := Query{Source: SourcePVMBG, Limit: q.Limit}
		if _, err := cache.read(ctx, other, time.Second, read); err != nil || calls.Load() != 2 {
			t.Fatalf("different filters used %d store calls, error %v", calls.Load(), err)
		}

		time.Sleep(time.Second)

		if _, err := cache.read(ctx, q, time.Second, read); err != nil || calls.Load() != 3 {
			t.Fatalf("expired page used %d store calls, error %v", calls.Load(), err)
		}
	})
}

func TestHazardReadCacheBypassesIncompleteAndUnboundedQueries(t *testing.T) {
	partial := testHazardPage()
	partial.Partial = true
	unavailable := Response{Sources: []Freshness{{Status: "unavailable"}}}
	now := time.Now()

	tests := []struct {
		name string
		q    Query
		page Response
		err  error
		ttl  time.Duration
	}{
		{"partial", Query{Limit: 20}, partial, nil, time.Second},
		{"unavailable", Query{Limit: 20}, unavailable, nil, time.Second},
		{"error", Query{Limit: 20}, Response{}, errors.New("read failed"), time.Second},
		{"audit", Query{Limit: 20, Audit: true}, testHazardPage(), nil, time.Second},
		{"cursor", Query{Limit: 20, Cursor: "first-page"}, testHazardPage(), nil, time.Second},
		{"since", Query{Limit: 20, Since: &now}, testHazardPage(), nil, time.Second},
		{"large page", Query{Limit: hazardCacheMaxLimit + 1}, testHazardPage(), nil, time.Second},
		{"disabled", Query{Limit: 20}, testHazardPage(), nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cache hazardReadCache

			calls := 0
			read := func(context.Context, Query) (Response, error) {
				calls++

				return tc.page, tc.err
			}

			for range 2 {
				if _, err := cache.read(context.Background(), tc.q, tc.ttl, read); !errors.Is(err, tc.err) {
					t.Fatalf("read returned %v, want %v", err, tc.err)
				}
			}

			if calls != 2 {
				t.Fatalf("used %d store calls, want 2", calls)
			}
		})
	}
}

func TestHazardReadCacheWaiterCanRecoverFromCanceledRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			cache hazardReadCache
			calls atomic.Int32
		)

		miss, cancel := context.WithCancel(context.Background())
		defer cancel()

		started := make(chan struct{})
		read := func(ctx context.Context, _ Query) (Response, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-ctx.Done()

				return Response{}, ctx.Err()
			}

			return testHazardPage(), nil
		}

		q := Query{Limit: 20}
		ownerDone, waiterDone := make(chan error, 1), make(chan error, 1)

		go func() {
			_, err := cache.read(miss, q, time.Second, read)
			ownerDone <- err
		}()

		<-started

		go func() {
			_, err := cache.read(context.Background(), q, time.Second, read)
			waiterDone <- err
		}()

		synctest.Wait()
		cancel()

		if err := <-ownerDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("initial reader error %v, want context canceled", err)
		}

		if err := <-waiterDone; err != nil || calls.Load() != 2 {
			t.Fatalf("waiter recovery error %v, store calls %d", err, calls.Load())
		}
	})
}

func testHazardPage() Response {
	return Response{Sources: []Freshness{{Source: SourceBMKG, Status: "healthy"}}}
}
