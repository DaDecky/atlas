package placement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// CachedConfigMap removes GETs from the data path. Only mutations reach the API;
// conflicts reload the authoritative snapshot before the caller retries its CAS.
type CachedConfigMap struct {
	Remote  *ConfigMap
	mu      sync.RWMutex
	table   Table
	version string
	ready   bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func WatchConfigMap(ctx context.Context, remote *ConfigMap) (*CachedConfigMap, error) {
	t, version, err := remote.Load(ctx)
	if err != nil {
		return nil, err
	}
	// The caller's startup deadline does not own the lifetime of the watch.
	lifetime, cancel := context.WithCancel(context.Background())

	c := &CachedConfigMap{Remote: remote, table: t, version: version, ready: true, cancel: cancel, done: make(chan struct{})}
	go c.run(lifetime)

	return c, nil
}

func cloneTable(t Table) Table {
	clone := t
	clone.Nodes = append([]string(nil), t.Nodes...)

	clone.Shards = make(map[string][]Copy, len(t.Shards))
	for shard, holders := range t.Shards {
		clone.Shards[shard] = append([]Copy(nil), holders...)
	}

	clone.WriterEpochs = make(map[string]int64, len(t.WriterEpochs))
	for source, epoch := range t.WriterEpochs {
		clone.WriterEpochs[source] = epoch
	}

	return clone
}

func (c *CachedConfigMap) Load(ctx context.Context) (Table, string, error) {
	if err := ctx.Err(); err != nil {
		return Table{}, "", err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.ready {
		return Table{}, "", errors.New("placement watch disconnected")
	}

	return cloneTable(c.table), c.version, nil
}

func (c *CachedConfigMap) update(t Table, version string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Watch notifications can arrive after a newer PUT response. Generations are
	// our monotonic counter; Kubernetes resourceVersion itself is opaque.
	if t.Generation >= c.table.Generation {
		c.table, c.version = cloneTable(t), version
	}
}

func (c *CachedConfigMap) CompareAndSwap(ctx context.Context, version string, t Table) error {
	v, err := c.Remote.swap(ctx, version, t)
	if err == nil {
		c.update(t, v)
		return nil
	}

	if errors.Is(err, ErrConflict) {
		fresh, v, loadErr := c.Remote.Load(ctx)
		if loadErr != nil {
			return errors.Join(err, loadErr)
		}

		c.update(fresh, v)
	}

	return err
}

func (c *CachedConfigMap) Close() {
	c.cancel()
	<-c.done
}

func (c *CachedConfigMap) setReady(ready bool) {
	c.mu.Lock()
	c.ready = ready
	c.mu.Unlock()
}

func (c *CachedConfigMap) run(ctx context.Context) {
	defer close(c.done)

	for ctx.Err() == nil {
		started := time.Now()
		_ = c.watch(ctx)
		// Do not keep serving a placement snapshot after losing its watch.
		c.setReady(false)
		// Back off short-lived failures; a normally expired 30 s watch reconnects
		// immediately rather than causing a one-second outage every half minute.
		if delay := time.Second - time.Since(started); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		// A full reload also handles expired resource versions (HTTP/event 410).
		for ctx.Err() == nil {
			t, v, err := c.Remote.Load(ctx)
			if err == nil {
				c.update(t, v)
				break
			}

			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (c *CachedConfigMap) watch(ctx context.Context) error {
	c.mu.RLock()
	version := c.version
	c.mu.RUnlock()
	query := url.Values{"watch": {"true"}, "fieldSelector": {"metadata.name=" + c.Remote.Name}, "resourceVersion": {version}, "timeoutSeconds": {"30"}}
	endpoint := strings.TrimSuffix(c.Remote.endpoint(), "/"+url.PathEscape(c.Remote.Name)) + "?" + query.Encode()

	call, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(call, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+c.Remote.Token)
	// Streaming watches outlive the ordinary GET/PUT client timeout.
	client := *c.Remote.Client
	client.Timeout = 0

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("placement watch status %d", resp.StatusCode)
	}

	c.setReady(true)

	decoder := json.NewDecoder(resp.Body)

	for {
		var event struct {
			Type   string    `json:"type"`
			Object configMap `json:"object"`
		}
		if err = decoder.Decode(&event); err != nil {
			return err
		}

		switch event.Type {
		case "ADDED", "MODIFIED":
			var t Table
			if err = json.Unmarshal([]byte(event.Object.Data["placement.json"]), &t); err != nil {
				return err
			}

			if err = t.Validate(); err != nil {
				return err
			}

			c.update(t, event.Object.Metadata.ResourceVersion)
		case "ERROR", "DELETED":
			return errors.New("placement watch reset")
		}
	}
}
