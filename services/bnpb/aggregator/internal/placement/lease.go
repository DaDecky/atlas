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
	"time"
)

// Lease protects refill/rebalance planning. Expiry is measured from observing an
// unchanged resourceVersion with a local monotonic clock, not a remote wall time.
type Lease struct {
	Client                                 *http.Client
	Origin, Namespace, Name, Token, Holder string
	Duration                               time.Duration
	observed                               string
	since                                  time.Time
}
type leaseDocument struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec struct {
		HolderIdentity       string `json:"holderIdentity"`
		LeaseDurationSeconds int    `json:"leaseDurationSeconds"`
		RenewTime            string `json:"renewTime"`
	} `json:"spec"`
}

var ErrLeaseHeld = errors.New("placement lease held by another planner")

func (l *Lease) Acquire(ctx context.Context) error {
	endpoint := l.Origin + "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(l.Namespace) + "/leases"
	request := func(method, path string, body io.Reader) (*http.Response, error) {
		r, err := http.NewRequestWithContext(ctx, method, path, body)
		if err != nil {
			return nil, err
		}

		r.Header.Set("Authorization", "Bearer "+l.Token)
		r.Header.Set("Content-Type", "application/json")

		return l.Client.Do(r)
	}

	response, err := request("GET", endpoint+"/"+url.PathEscape(l.Name), nil)
	if err != nil {
		return err
	}

	var document leaseDocument

	status := response.StatusCode
	if status == 200 {
		err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document)
	}

	_ = response.Body.Close()

	if err != nil {
		return err
	}

	if status != 200 && status != 404 {
		return fmt.Errorf("placement lease GET status %d", status)
	}

	duration := l.Duration
	if duration <= 0 {
		duration = 15 * time.Second
	}

	if status == 200 && document.Spec.HolderIdentity != "" && document.Spec.HolderIdentity != l.Holder {
		if l.observed != document.Metadata.ResourceVersion {
			l.observed = document.Metadata.ResourceVersion
			l.since = time.Now()

			return ErrLeaseHeld
		}

		wait := time.Duration(document.Spec.LeaseDurationSeconds) * time.Second
		if wait < duration {
			wait = duration
		}

		if time.Since(l.since) < wait {
			return ErrLeaseHeld
		}
	}

	document.APIVersion = "coordination.k8s.io/v1"
	document.Kind = "Lease"
	document.Metadata.Name = l.Name
	document.Spec.HolderIdentity = l.Holder
	document.Spec.LeaseDurationSeconds = int(duration / time.Second)
	document.Spec.RenewTime = time.Now().UTC().Format(time.RFC3339Nano)

	raw, err := json.Marshal(document)
	if err != nil {
		return err
	}

	method, path := "PUT", endpoint+"/"+url.PathEscape(l.Name)
	if status == 404 {
		method, path = "POST", endpoint
	}

	response, err = request(method, path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode == 409 {
		return ErrLeaseHeld
	}

	if response.StatusCode != 200 && response.StatusCode != 201 {
		return fmt.Errorf("placement lease update status %d", response.StatusCode)
	}

	return nil
}
