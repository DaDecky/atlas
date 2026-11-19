package placement

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLeaseUsesKubernetesMicroTimeForCreateAndRenew(t *testing.T) {
	var document leaseDocument

	updates := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-api" {
			w.WriteHeader(401)
			return
		}

		if r.Method == "GET" {
			if updates == 0 {
				w.WriteHeader(404)
			} else {
				_ = json.NewEncoder(w).Encode(document)
			}

			return
		}

		if json.NewDecoder(r.Body).Decode(&document) != nil {
			w.WriteHeader(400)
			return
		}
		// This is apimachinery's MicroTime wire grammar, including the fixed
		// six fractional digits. A permissive RFC3339Nano fake misses the bug.
		if _, err := time.Parse("2006-01-02T15:04:05.000000Z07:00", document.Spec.RenewTime); err != nil {
			w.WriteHeader(400)
			return
		}

		updates++
		document.Metadata.ResourceVersion = "updated"

		w.WriteHeader(201)
	}))
	defer server.Close()

	lease := &Lease{Client: server.Client(), Origin: server.URL, Namespace: "test", Name: "recovery", Token: "test-api", Holder: "owner"}
	for range 10 {
		if err := lease.Acquire(context.Background()); err != nil {
			t.Fatal("Kubernetes MicroTime create/renew rejected", err)
		}
	}

	if updates != 10 {
		t.Fatal("lease did not renew", updates)
	}
}
