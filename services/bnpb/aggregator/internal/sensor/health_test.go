package sensor

import (
	"testing"
	"time"
)

var generated = time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)

func item(id string, age time.Duration) Item {
	return Item{Observation: Observation{ID: id, ObservedAt: generated.Add(-age)}}
}

func TestCleanPageIsHealthy(t *testing.T) {
	got, last, health := Assess(Page{GeneratedAt: generated, Items: []Item{item("a", 3*time.Second), item("b", time.Second)}}, Position{}, time.Minute)
	if len(got) != 2 || health.Degraded() || last.ObservationID != "b" {
		t.Fatalf("items %d, last %+v, health %+v", len(got), last, health)
	}
}

func TestRepeatsAreDroppedAndMarkDegraded(t *testing.T) {
	last := Position{ObservedAt: generated.Add(-5 * time.Second), ObservationID: "a"}

	got, _, health := Assess(Page{GeneratedAt: generated, Items: []Item{item("a", 5*time.Second), item("b", time.Second), item("b", time.Second)}}, last, time.Minute)
	if len(got) != 1 || health.Repeated != 2 || !health.Degraded() {
		t.Fatalf("items %d, health %+v", len(got), health)
	}
}

func TestOutOfOrderReadingIsKeptAndMarked(t *testing.T) {
	got, last, health := Assess(Page{GeneratedAt: generated, Items: []Item{item("b", time.Second), item("a", 9*time.Second)}}, Position{}, time.Minute)
	if len(got) != 2 || health.OutOfOrder != 1 || last.ObservationID != "b" {
		t.Fatalf("items %d, last %+v, health %+v", len(got), last, health)
	}
}

func TestNewestReadingPastBoundIsStale(t *testing.T) {
	_, _, health := Assess(Page{GeneratedAt: generated, Items: []Item{item("a", 2*time.Hour), item("b", time.Hour)}}, Position{}, 5*time.Minute)
	if !health.Stale {
		t.Fatalf("health %+v", health)
	}

	_, _, health = Assess(Page{GeneratedAt: generated, Items: []Item{item("a", 2*time.Hour), item("b", time.Second)}}, Position{}, 5*time.Minute)
	if health.Stale {
		t.Fatal("fresh newest reading marked stale")
	}
}

func TestViolationMarksDegraded(t *testing.T) {
	_, _, health := Assess(Page{GeneratedAt: generated, Items: []Item{{Violation: "missing field"}}}, Position{}, time.Minute)
	if health.Violations != 1 || !health.Degraded() {
		t.Fatalf("health %+v", health)
	}
}
