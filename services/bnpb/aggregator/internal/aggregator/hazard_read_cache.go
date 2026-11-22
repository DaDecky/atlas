package aggregator

import (
	"context"
	"sync"
	"time"
)

type hazardPage struct {
	response Response
	expires  time.Time
}

type hazardRead struct {
	done     chan struct{}
	response Response
	err      error
	canceled bool
}

// hazardReadCache serves repeated first-page reads within a short freshness window.
// Identical concurrent misses share one store read.
type hazardReadCache struct {
	mu      sync.Mutex
	pages   map[Query]hazardPage
	pending map[Query]*hazardRead
}

const (
	hazardCacheMaxPages = 64
	hazardCacheMaxLimit = 100
)

func (c *hazardReadCache) read(ctx context.Context, q Query, ttl time.Duration, load func(context.Context, Query) (Response, error)) (Response, error) {
	if ttl <= 0 || q.Audit || q.Cursor != "" || q.Since != nil || q.Limit > hazardCacheMaxLimit {
		return load(ctx, q)
	}

	start := time.Now()

	c.mu.Lock()
	if page, ok := c.pages[q]; ok && start.Before(page.expires) {
		c.mu.Unlock()

		return page.response, nil
	}

	if active := c.pending[q]; active != nil {
		c.mu.Unlock()

		return waitForHazardRead(ctx, q, active, load)
	}

	if c.pending == nil {
		c.pending = make(map[Query]*hazardRead)
	}

	active := &hazardRead{done: make(chan struct{}), canceled: true}
	c.pending[q] = active
	c.mu.Unlock()

	defer c.finish(q, active, start.Add(ttl))

	active.response, active.err = load(ctx, q)
	active.canceled = ctx.Err() != nil

	return active.response, active.err
}

func (c *hazardReadCache) finish(q Query, active *hazardRead, expires time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if active.err == nil && !active.canceled && completeHazardPage(active.response) {
		c.remember(q, active.response, expires, time.Now())
	}

	delete(c.pending, q)
	close(active.done)
}

func waitForHazardRead(ctx context.Context, q Query, active *hazardRead, load func(context.Context, Query) (Response, error)) (Response, error) {
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case <-active.done:
		if err := ctx.Err(); err != nil {
			return Response{}, err
		}

		// A canceled request must not shorten another caller's read deadline.
		if active.canceled {
			return load(ctx, q)
		}

		return active.response, active.err
	}
}

func completeHazardPage(result Response) bool {
	if result.Partial || len(result.UnavailableUnits) > 0 {
		return false
	}

	for _, source := range result.Sources {
		if source.Status != "unavailable" {
			return true
		}
	}

	return false
}

// remember runs with c.mu held. Expired entries make space for new query shapes.
func (c *hazardReadCache) remember(q Query, result Response, expires, now time.Time) {
	if !expires.After(now) {
		return
	}

	if len(c.pages) >= hazardCacheMaxPages {
		for key, page := range c.pages {
			if !page.expires.After(now) {
				delete(c.pages, key)
			}
		}
	}

	if len(c.pages) >= hazardCacheMaxPages {
		return
	}

	if c.pages == nil {
		c.pages = make(map[Query]hazardPage)
	}

	c.pages[q] = hazardPage{response: result, expires: expires}
}
