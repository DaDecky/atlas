package placement

import (
	"context"
	"errors"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// CachedConfigMap serves reads from a client-go informer that watches the one placement
// ConfigMap, so only writes reach the API server. A dropped watch stops reads until the
// informer lists the ConfigMap again.
type CachedConfigMap struct {
	Remote  *ConfigMap
	mu      sync.RWMutex
	table   Table
	version string
	ready   bool
	factory informers.SharedInformerFactory
	stop    chan struct{}
}

func WatchConfigMap(ctx context.Context, remote *ConfigMap) (*CachedConfigMap, error) {
	c := &CachedConfigMap{Remote: remote, stop: make(chan struct{})}
	// RBAC grants list and watch on this one name only, and asks for the matching field selector.
	c.factory = informers.NewSharedInformerFactoryWithOptions(remote.client, 0,
		informers.WithNamespace(remote.Namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("metadata.name", remote.Name).String()
		}))

	informer := c.factory.Core().V1().ConfigMaps().Informer()
	if err := informer.SetWatchErrorHandler(func(*cache.Reflector, error) { c.setReady(false) }); err != nil {
		return nil, fmt.Errorf("watch placement: %w", err)
	}

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.observe,
		UpdateFunc: func(_, obj any) { c.observe(obj) },
		DeleteFunc: func(any) { c.setReady(false) },
	})
	if err != nil {
		return nil, fmt.Errorf("watch placement: %w", err)
	}

	c.factory.Start(c.stop)

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		c.Close()

		return nil, errors.New("placement watch did not sync")
	}

	if _, _, err = c.Load(ctx); err != nil {
		c.Close()

		return nil, err
	}

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

func (c *CachedConfigMap) CompareAndSwap(ctx context.Context, version string, t Table) error {
	v, err := c.Remote.swap(ctx, version, t)
	if err == nil {
		c.update(t, v)

		return nil
	}
	// A conflict reloads the authoritative table before the caller retries.
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
	close(c.stop)
	c.factory.Shutdown()
}

// observe takes a watched ConfigMap. An invalid table stops reads instead of serving it.
func (c *CachedConfigMap) observe(obj any) {
	cm := obj.(*corev1.ConfigMap)

	t, err := decode(cm)
	if err != nil {
		c.setReady(false)
		return
	}

	c.update(t, cm.ResourceVersion)
}

func (c *CachedConfigMap) update(t Table, version string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Watch events can arrive after a newer write response. Generation is our monotonic
	// counter, since the resourceVersion of Kubernetes is opaque.
	if !c.ready || t.Generation >= c.table.Generation {
		c.table, c.version = cloneTable(t), version
	}

	c.ready = true
}

func (c *CachedConfigMap) setReady(ready bool) {
	c.mu.Lock()
	c.ready = ready
	c.mu.Unlock()
}
