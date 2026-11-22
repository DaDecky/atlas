package main

import (
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/aggregator"
)

func TestWriteTimeoutCoversAuditTimeout(t *testing.T) {
	c := aggregator.Config{QueryTimeout: 2 * time.Second, AuditTimeout: 10 * time.Second}
	if got := newServer(c, nil).WriteTimeout; got <= c.AuditTimeout {
		t.Fatalf("WriteTimeout = %s, want more than AUDIT_TIMEOUT %s", got, c.AuditTimeout)
	}

	c = aggregator.Config{QueryTimeout: 30 * time.Second, AuditTimeout: 10 * time.Second}
	if got := newServer(c, nil).WriteTimeout; got <= c.QueryTimeout {
		t.Fatalf("WriteTimeout = %s, want more than QUERY_TIMEOUT %s", got, c.QueryTimeout)
	}
}
