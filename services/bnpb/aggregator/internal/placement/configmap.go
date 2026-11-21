package placement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

type Repository interface {
	Load(context.Context) (Table, string, error)
	CompareAndSwap(context.Context, string, Table) error
}

// ConfigMap keeps the placement table in one ConfigMap, under the key placement.json.
type ConfigMap struct {
	client          kubernetes.Interface
	Namespace, Name string
}

// Each call to the API server has its own deadline, since the watch shares the client.
const callTimeout = 5 * time.Second

var ErrConflict = errors.New("placement version conflict")

// NewConfigMap connects through a token file, which client-go reloads as the kubelet rotates it.
func NewConfigMap(origin, tokenFile, caFile, namespace, name string) (*ConfigMap, error) {
	config := &rest.Config{Host: origin, BearerTokenFile: tokenFile, TLSClientConfig: rest.TLSClientConfig{CAFile: caFile}}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create placement client: %w", err)
	}

	return &ConfigMap{client: client, Namespace: namespace, Name: name}, nil
}

func (c *ConfigMap) Load(ctx context.Context) (Table, string, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	cm, err := c.client.CoreV1().ConfigMaps(c.Namespace).Get(ctx, c.Name, metav1.GetOptions{})
	if err != nil {
		return Table{}, "", fmt.Errorf("read placement: %w", err)
	}

	t, err := decode(cm)

	return t, cm.ResourceVersion, err
}

func (c *ConfigMap) CompareAndSwap(ctx context.Context, version string, t Table) error {
	_, err := c.swap(ctx, version, t)

	return err
}

// swap writes t only if the ConfigMap is still at version, and returns the new version.
func (c *ConfigMap) swap(ctx context.Context, version string, t Table) (string, error) {
	if err := t.Validate(); err != nil {
		return "", err
	}

	raw, err := json.Marshal(t)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: c.Name, Namespace: c.Namespace, ResourceVersion: version},
		Data:       map[string]string{"placement.json": string(raw)},
	}

	updated, err := c.client.CoreV1().ConfigMaps(c.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return "", ErrConflict
	}

	if err != nil {
		return "", fmt.Errorf("write placement: %w", err)
	}

	return updated.ResourceVersion, nil
}

func decode(cm *corev1.ConfigMap) (Table, error) {
	var t Table
	if err := json.Unmarshal([]byte(cm.Data["placement.json"]), &t); err != nil {
		return t, fmt.Errorf("decode placement: %w", err)
	}

	return t, t.Validate()
}

// Update applies change to the newest table and swaps it in. A conflict means another writer
// swapped first, so it retries with the backoff of client-go, and change rechecks its own rules.
// change returns false when the table already holds what it wants.
func Update(ctx context.Context, repo Repository, change func(*Table) (bool, error)) error {
	return retry.OnError(retry.DefaultRetry, func(err error) bool { return errors.Is(err, ErrConflict) }, func() error {
		t, version, err := repo.Load(ctx)
		if err != nil {
			return err
		}

		changed, err := change(&t)
		if err != nil || !changed {
			return err
		}

		t.Generation++

		return repo.CompareAndSwap(ctx, version, t)
	})
}
