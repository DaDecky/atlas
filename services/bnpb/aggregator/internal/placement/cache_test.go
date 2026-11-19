package placement

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// fakeAPI stands in for the Kubernetes API server, which is outside the project. The fake
// clientset ignores resourceVersion on update, so a reactor refuses a stale one like the API does.
func fakeAPI(t *testing.T) (*fake.Clientset, *ConfigMap) {
	t.Helper()

	table, err := New([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}

	client := fake.NewClientset(configMapOf(t, table, "1"))
	client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)

		current, err := client.Tracker().Get(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "test", "placement")
		if err != nil {
			return true, nil, err
		}

		stored := current.(*corev1.ConfigMap).ResourceVersion
		if update.ResourceVersion != stored {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "placement", errors.New("stale version"))
		}

		next, _ := strconv.Atoi(stored)
		update.ResourceVersion = strconv.Itoa(next + 1)

		return false, nil, nil
	})

	return client, &ConfigMap{client: client, Namespace: "test", Name: "placement"}
}

func configMapOf(t *testing.T, table Table, version string) *corev1.ConfigMap {
	t.Helper()

	raw, err := json.Marshal(table)
	if err != nil {
		t.Fatal(err)
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "placement", Namespace: "test", ResourceVersion: version},
		Data:       map[string]string{"placement.json": string(raw)},
	}
}

// write stands for another writer, which swaps the table at the current version.
func write(t *testing.T, client *fake.Clientset, table Table) {
	t.Helper()

	current, err := client.CoreV1().ConfigMaps("test").Get(context.Background(), "placement", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err = client.CoreV1().ConfigMaps("test").Update(context.Background(), configMapOf(t, table, current.ResourceVersion), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func watched(t *testing.T, remote *ConfigMap) *CachedConfigMap {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cached, err := WatchConfigMap(ctx, remote)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(cached.Close)

	return cached
}

func eventually(t *testing.T, ok func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func countGets(client *fake.Clientset) int {
	gets := 0

	for _, action := range client.Actions() {
		if action.GetVerb() == "get" {
			gets++
		}
	}

	return gets
}

func TestCacheServesReadsWithoutAPIRequests(t *testing.T) {
	client, remote := fakeAPI(t)
	cached := watched(t, remote)
	before := countGets(client)

	for range 100 {
		if _, _, err := cached.Load(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	if after := countGets(client); after != before {
		t.Fatalf("reads made %d API requests", after-before)
	}
}

func TestStaleSwapConflictsAndReloads(t *testing.T) {
	_, remote := fakeAPI(t)
	cached := watched(t, remote)

	table, version, err := cached.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	table.Generation++
	if err = cached.CompareAndSwap(context.Background(), version, table); err != nil {
		t.Fatal(err)
	}

	swapped := table.Generation

	table.Generation++
	if err = cached.CompareAndSwap(context.Background(), version, table); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale version swapped: %v", err)
	}

	if current, _, _ := cached.Load(context.Background()); current.Generation != swapped {
		t.Fatalf("cache generation %d, want %d", current.Generation, swapped)
	}
}

func TestWatchedChangeReachesCache(t *testing.T) {
	client, remote := fakeAPI(t)
	cached := watched(t, remote)

	table, _, _ := cached.Load(context.Background())
	table.Generation = 7
	write(t, client, table)

	eventually(t, func() bool {
		current, _, err := cached.Load(context.Background())
		return err == nil && current.Generation == 7
	})
}

func TestInvalidTableStopsReads(t *testing.T) {
	client, remote := fakeAPI(t)
	cached := watched(t, remote)

	write(t, client, Table{})

	eventually(t, func() bool {
		_, _, err := cached.Load(context.Background())
		return err != nil
	})
}

func TestUpdateRetriesAfterConflict(t *testing.T) {
	client, remote := fakeAPI(t)
	raced := false

	err := Update(context.Background(), remote, func(table *Table) (bool, error) {
		if !raced {
			raced = true
			// Another writer swaps first, so this attempt holds a stale version.
			other := cloneTable(*table)
			other.Generation = 40
			write(t, client, other)
		}

		table.WriterEpochs["BMKG"] = 9

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	table, _, err := remote.Load(context.Background())
	if err != nil || table.WriterEpochs["BMKG"] != 9 || table.Generation != 41 {
		t.Fatalf("table %+v, error %v", table.WriterEpochs, err)
	}
}
