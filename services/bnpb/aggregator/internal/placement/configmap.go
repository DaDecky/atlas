package placement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Repository interface {
	Load(context.Context) (Table, string, error)
	CompareAndSwap(context.Context, string, Table) error
}

type ConfigMap struct {
	Client                         *http.Client
	Origin, Namespace, Name, Token string
}

type configMap struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Data map[string]string `json:"data"`
}

var ErrConflict = errors.New("placement version conflict")

func (c *ConfigMap) endpoint() string {
	return c.Origin + "/api/v1/namespaces/" + url.PathEscape(c.Namespace) + "/configmaps/" + url.PathEscape(c.Name)
}

func (c *ConfigMap) request(ctx context.Context, method string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	return c.Client.Do(req)
}

func (c *ConfigMap) Load(ctx context.Context) (Table, string, error) {
	var t Table

	resp, err := c.request(ctx, http.MethodGet, nil)
	if err != nil {
		return t, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return t, "", fmt.Errorf("placement GET status %d", resp.StatusCode)
	}

	var cm configMap
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&cm); err != nil {
		return t, "", err
	}

	if err = json.Unmarshal([]byte(cm.Data["placement.json"]), &t); err != nil {
		return t, "", err
	}

	return t, cm.Metadata.ResourceVersion, t.Validate()
}

func (c *ConfigMap) CompareAndSwap(ctx context.Context, version string, t Table) error {
	_, err := c.swap(ctx, version, t)
	return err
}

func (c *ConfigMap) swap(ctx context.Context, version string, t Table) (string, error) {
	if err := t.Validate(); err != nil {
		return "", err
	}

	raw, err := json.Marshal(t)
	if err != nil {
		return "", err
	}

	cm := configMap{APIVersion: "v1", Kind: "ConfigMap", Data: map[string]string{"placement.json": string(raw)}}
	cm.Metadata.Name = c.Name
	cm.Metadata.ResourceVersion = version

	body, err := json.Marshal(cm)
	if err != nil {
		return "", err
	}

	resp, err := c.request(ctx, http.MethodPut, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 409 {
		return "", ErrConflict
	}

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("placement PUT status %d", resp.StatusCode)
	}

	var updated configMap
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&updated); err != nil {
		return "", err
	}

	if updated.Metadata.ResourceVersion == "" {
		return "", errors.New("placement PUT missing resource version")
	}

	return updated.Metadata.ResourceVersion, nil
}
