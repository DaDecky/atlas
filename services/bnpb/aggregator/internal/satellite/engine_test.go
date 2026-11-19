package satellite

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand/v2"
	"testing"
)

func detection(lon float64) Detection {
	return Detection{Satellite: "NOAA-20", AcqDate: "2026-11-18", AcqTime: "1200", Latitude: -6, Longitude: lon, Confidence: "nominal", FRP: 4}
}

func step(t *testing.T, e *Engine, kind string, d Detection) []Fire {
	t.Helper()

	offset, has := e.Position()
	if !has {
		offset = 0
	}

	frame := Frame{Offset: offset + 1, Type: kind, Detection: &d}
	if kind == "retract" {
		frame.Detection, frame.Key = nil, d.Key()
	}

	changes, err := e.Apply(frame)
	if err != nil {
		t.Fatal(err)
	}

	if err = e.Check(); err != nil {
		t.Fatal(err)
	}

	return changes
}

func active(e *Engine) []Fire {
	out := []Fire{}

	for _, fire := range e.Fires() {
		if fire.Status == "active" {
			out = append(out, fire)
		}
	}

	return out
}

func TestTransitiveCrossSatelliteDeduplicationAndConfidence(t *testing.T) {
	e := New()
	a, b, c := detection(106), detection(106.008), detection(106.016)
	b.Satellite, b.Confidence = "Suomi-NPP", "high"

	step(t, e, "detection", a)

	if e.Stats().Singletons != 1 || len(active(e)) != 0 {
		t.Fatal("singleton became a fire")
	}

	step(t, e, "detection", b)
	id := active(e)[0].ID
	step(t, e, "detection", c)
	step(t, e, "detection", a)

	low := detection(106.004)
	low.Confidence = "low"
	step(t, e, "detection", low)

	fires := active(e)
	if len(fires) != 1 || fires[0].ID != id || fires[0].Count != 3 || len(fires[0].Satellites) != 2 || fires[0].HighestConfidence != "high" || fires[0].TotalFRP != 12 || Connected(a, c) {
		t.Fatal("transitive cluster or duplicate/confidence rule", fires)
	}
}

func TestSpatialAndEventTimeBoundaries(t *testing.T) {
	a := detection(0)
	a.Latitude = 0
	b := a

	b.Longitude = 0.999 / EarthRadiusKM * 180 / math.Pi
	if !Connected(a, b) {
		t.Fatal("point within 1 km excluded")
	}

	b.Longitude = 1.001 / EarthRadiusKM * 180 / math.Pi
	if Connected(a, b) {
		t.Fatal("point beyond 1 km included")
	}

	b = a

	b.AcqTime = "1800"
	if !Connected(a, b) {
		t.Fatal("exactly 360 minutes excluded")
	}

	b.AcqTime = "1801"
	if Connected(a, b) {
		t.Fatal("beyond 360 minutes included")
	}

	for _, lat := range []float64{0, 89.999} {
		e := New()
		a.Latitude, a.Longitude = lat, 179.999
		b = a
		b.Longitude = -179.999

		step(t, e, "detection", a)
		step(t, e, "detection", b)

		if len(active(e)) != 1 {
			t.Fatal("spatial grid missed antimeridian/pole neighbour", lat)
		}
	}
}

func TestSeverityThresholds(t *testing.T) {
	for _, tc := range []struct {
		count int
		frp   float64
		want  string
	}{{2, 29.999, "WASPADA"}, {5, 30, "SIAGA"}, {6, 0, "SIAGA"}, {19, 99.999, "SIAGA"}, {20, 0, "AWAS"}, {2, 100, "AWAS"}} {
		if got := Severity(tc.count, tc.frp); got != tc.want {
			t.Fatal(tc, got)
		}
	}
}

func TestMintIdentitySurvivesEarlierArrivalAndEarliestRetraction(t *testing.T) {
	e := New()
	a, b := detection(106), detection(106.003)
	step(t, e, "detection", a)
	step(t, e, "detection", b)
	id := active(e)[0].ID
	earlier := detection(106.001)
	earlier.AcqTime = "1100"
	step(t, e, "detection", earlier)

	if fire := active(e)[0]; fire.ID != id || fire.OccurredAt.Hour() != 11 {
		t.Fatal("late earlier member changed minted identity", fire)
	}

	step(t, e, "retract", earlier)

	if fire := active(e)[0]; fire.ID != id || fire.Count != 2 || fire.OccurredAt.Hour() != 12 {
		t.Fatal("earliest retraction changed identity", fire)
	}
}

func TestMergeSplitWithdrawalAndRevival(t *testing.T) {
	e := New()
	a, b, c, d := detection(106), detection(106.006), detection(106.018), detection(106.024)
	step(t, e, "detection", a)
	step(t, e, "detection", b)
	firstID := active(e)[0].ID
	step(t, e, "detection", c)
	step(t, e, "detection", d)

	if len(active(e)) != 2 {
		t.Fatal("separate groups merged early")
	}

	bridge := detection(106.012)
	step(t, e, "detection", bridge)

	if fires := active(e); len(fires) != 1 || fires[0].ID != firstID || fires[0].Count != 5 {
		t.Fatal("merge did not retain older equal-sized identity", fires)
	}

	step(t, e, "retract", bridge)

	if fires := active(e); len(fires) != 2 {
		t.Fatal("split did not produce two groups", fires)
	}

	for _, fire := range e.Fires() {
		if fire.Status != "active" && (fire.Count != 0 || len(fire.Keys) != 0) {
			t.Fatal("inactive event owns detections", fire)
		}
	}

	step(t, e, "retract", a)

	if e.state.Fires[firstID].Status != "withdrawn" || e.Stats().Singletons != 1 {
		t.Fatal("singleton failed to withdraw original fire", e.Fires())
	}

	step(t, e, "update", a)

	if e.state.Fires[firstID].Status != "active" || e.state.Fires[firstID].Count != 2 {
		t.Fatal("withdrawn identity not revived", e.Fires())
	}
}

func TestLateDataRetainedAndOldCorrectionsStillApply(t *testing.T) {
	e := New()
	a, b := detection(106), detection(106.003)
	step(t, e, "detection", a)
	step(t, e, "detection", b)
	id := active(e)[0].ID
	newer := detection(107)
	newer.AcqTime = "1900"
	step(t, e, "detection", newer)

	late := detection(106.001)
	late.AcqTime = "1100"
	step(t, e, "detection", late)
	step(t, e, "detection", late)

	if e.Stats().LateCount != 1 || len(e.state.Late) != 1 || e.state.Fires[id].Count != 2 {
		t.Fatal("late detection silently lost, counted twice, or admitted", e.Stats())
	}

	a.FRP = 110
	step(t, e, "update", a)

	if e.state.Fires[id].Severity != "AWAS" {
		t.Fatal("known old correction rejected by lateness gate")
	}

	step(t, e, "retract", b)

	if e.state.Fires[id].Status != "withdrawn" {
		t.Fatal("old retraction ignored")
	}
}

func TestOffsetsValidationAndEveryCrashCutIsDeterministic(t *testing.T) {
	frames := []Frame{}

	for i, lon := range []float64{106, 106.006, 106.018, 106.024, 106.012} {
		d := detection(lon)
		frames = append(frames, Frame{Offset: int64(i + 1), Type: "detection", Detection: &d})
	}

	frames = append(frames, Frame{Offset: 6, Type: "retract", Key: frames[4].Detection.Key()}, Frame{Offset: 7, Type: "heartbeat"})

	baseline := New()
	for _, frame := range frames {
		if _, err := baseline.Apply(frame); err != nil {
			t.Fatal(err)
		}
	}

	want, _ := baseline.Snapshot()

	for cut := 0; cut <= len(frames); cut++ {
		e := New()
		for _, frame := range frames[:cut] {
			if _, err := e.Apply(frame); err != nil {
				t.Fatal(err)
			}
		}

		raw, _ := e.Snapshot()

		restarted, err := Restore(raw)
		if err != nil {
			t.Fatal(err)
		}
		// The server may replay the last committed frame inclusively.
		start := max(0, cut-1)
		for _, frame := range frames[start:] {
			if _, err = restarted.Apply(frame); err != nil {
				t.Fatal(err)
			}
		}

		got, _ := restarted.Snapshot()
		if !bytes.Equal(got, want) {
			t.Fatalf("crash cut %d changed result", cut)
		}
	}

	before, _ := baseline.Snapshot()
	if _, err := baseline.Apply(Frame{Offset: 9, Type: "heartbeat"}); err == nil {
		t.Fatal("offset gap accepted")
	}

	after, _ := baseline.Snapshot()
	if !bytes.Equal(before, after) {
		t.Fatal("invalid frame changed state")
	}
}

func TestRandomCorrectionsMatchIndependentPairwiseComponents(t *testing.T) {
	e := New()
	rng := rand.New(rand.NewPCG(123, 456))

	points := make([]Detection, 40)
	for i := range points {
		points[i] = detection(106 + float64(i)*0.003)
	}

	for range 250 {
		d := points[rng.IntN(len(points))]

		kind := []string{"detection", "update", "retract"}[rng.IntN(3)]
		if rng.IntN(3) == 0 {
			d.Confidence = "low"
		}

		step(t, e, kind, d)

		keys := []string{}

		for key, record := range e.state.Records {
			if eligible(record) {
				keys = append(keys, key)
			}
		}

		seen := map[string]bool{}
		groups := map[string]bool{}

		for _, key := range keys {
			if seen[key] {
				continue
			}

			group := []string{key}
			seen[key] = true

			for index := 0; index < len(group); index++ {
				for _, other := range keys {
					if !seen[other] && Connected(e.state.Records[group[index]].Detection, e.state.Records[other].Detection) {
						seen[other] = true
						group = append(group, other)
					}
				}
			}

			if len(group) >= 2 {
				for _, member := range group {
					groups[member] = true
				}

				owner := e.state.Owners[group[0]]
				if len(e.state.Fires[owner].Keys) != len(group) {
					t.Fatal("grid component differs from pairwise reference")
				}

				for _, member := range group {
					if e.state.Owners[member] != owner {
						t.Fatal("connected detections have different owners")
					}
				}
			}
		}

		if e.Stats().Singletons != len(keys)-len(groups) {
			t.Fatal("singleton count differs from pairwise reference")
		}

		if rng.IntN(10) == 0 {
			raw, _ := json.Marshal(e.state)

			var err error

			e, err = Restore(raw)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}
