package satellite

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
)

// The ephemeral spatial index uses unit-km cubes in Earth-centred coordinates
// and six-hour time buckets. Every point within 1 km lies in a neighbouring
// cube, including points across the antimeridian and near a pole.
type cell struct{ X, Y, Z, T int64 }

type Engine struct {
	state State
	grid  map[cell]map[string]bool
	edges map[string]map[string]bool
}

type Stats struct {
	Offset, LateCount      int64
	Singletons, Active     int
	Detections, Tombstones int
}

func New() *Engine {
	return &Engine{state: State{Version: 1, Records: map[string]Record{}, Tombstones: map[string]bool{}, Owners: map[string]string{}, Fires: map[string]Fire{}, Late: []LateRecord{}}, grid: map[cell]map[string]bool{}, edges: map[string]map[string]bool{}}
}

func Restore(raw []byte) (*Engine, error) {
	e := New()
	if err := json.Unmarshal(raw, &e.state); err != nil || e.state.Version != 1 || e.state.Offset < 0 ||
		e.state.Records == nil || e.state.Tombstones == nil || e.state.Owners == nil || e.state.Fires == nil {
		return nil, errors.New("invalid satellite checkpoint")
	}

	keys := make([]string, 0, len(e.state.Records))
	for key, record := range e.state.Records {
		if record.Detection.Validate() != nil || record.Detection.Key() != key {
			return nil, errors.New("invalid checkpoint detection")
		}

		if eligible(record) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	for _, key := range keys {
		e.add(key)
	}

	if err := e.Check(); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Engine) Snapshot() ([]byte, error) { return json.Marshal(e.state) }

func (e *Engine) Position() (int64, bool) { return e.state.Offset, e.state.HasOffset }

func eligible(record Record) bool {
	return !record.Retracted && !record.Late && record.Detection.Confidence != "low"
}

func bucket(d Detection) cell {
	lat, lon := d.Latitude*math.Pi/180, d.Longitude*math.Pi/180
	t, _ := d.Time()

	return cell{
		int64(math.Floor(EarthRadiusKM * math.Cos(lat) * math.Cos(lon))),
		int64(math.Floor(EarthRadiusKM * math.Cos(lat) * math.Sin(lon))),
		int64(math.Floor(EarthRadiusKM * math.Sin(lat))),
		t.Unix() / int64(Window.Seconds()),
	}
}

func (e *Engine) neighbours(d Detection) []string {
	c := bucket(d)
	keys := []string{}

	for x := int64(-1); x <= 1; x++ {
		for y := int64(-1); y <= 1; y++ {
			for z := int64(-1); z <= 1; z++ {
				for t := int64(-1); t <= 1; t++ {
					for key := range e.grid[cell{c.X + x, c.Y + y, c.Z + z, c.T + t}] {
						if Connected(d, e.state.Records[key].Detection) {
							keys = append(keys, key)
						}
					}
				}
			}
		}
	}

	sort.Strings(keys)

	return keys
}

func (e *Engine) add(key string) {
	d := e.state.Records[key].Detection

	edges := map[string]bool{}
	for _, other := range e.neighbours(d) {
		edges[other] = true
		e.edges[other][key] = true
	}

	e.edges[key] = edges

	c := bucket(d)
	if e.grid[c] == nil {
		e.grid[c] = map[string]bool{}
	}

	e.grid[c][key] = true
}

func (e *Engine) remove(key string) {
	for other := range e.edges[key] {
		delete(e.edges[other], key)
	}

	delete(e.edges, key)
	c := bucket(e.state.Records[key].Detection)
	delete(e.grid[c], key)

	if len(e.grid[c]) == 0 {
		delete(e.grid, c)
	}
}

func (e *Engine) component(key string, into map[string]bool) {
	if _, exists := e.edges[key]; !exists || into[key] {
		return
	}

	queue := []string{key}
	into[key] = true

	for len(queue) > 0 {
		current := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		for other := range e.edges[current] {
			if !into[other] {
				into[other] = true
				queue = append(queue, other)
			}
		}
	}
}

// Apply is atomic for validation failures. The caller must commit Snapshot and
// returned fire changes together, and reload the last durable snapshot on any
// uncertain commit. Duplicate stream offsets have no effect.
func (e *Engine) Apply(frame Frame) ([]Fire, error) {
	if frame.Offset < 0 {
		return nil, errors.New("negative stream offset")
	}

	if e.state.HasOffset && frame.Offset <= e.state.Offset {
		return nil, nil
	}

	if e.state.HasOffset && frame.Offset != e.state.Offset+1 || !e.state.HasOffset && frame.Offset > 1 {
		return nil, errors.New("stream offset gap")
	}

	frame, err := e.completeUpdate(frame)
	if err != nil {
		return nil, err
	}

	key, err := frameKey(frame)
	if err != nil {
		return nil, err
	}

	old, exists := e.state.Records[key]
	if frame.Type == "heartbeat" || frame.Type == "detection" && (exists || e.state.Tombstones[key]) {
		e.state.Offset, e.state.HasOffset = frame.Offset, true
		return nil, nil
	}

	affected := map[string]bool{}
	if exists && eligible(old) {
		e.component(key, affected)
		e.remove(key)
	}

	if frame.Type == "retract" {
		e.state.Tombstones[key] = true

		if exists {
			old.Retracted = true
			e.state.Records[key] = old
		}
	} else {
		e.record(frame, key, old, exists, affected)
	}

	changes := e.reconcile(affected, frame.Offset)
	e.state.Offset, e.state.HasOffset = frame.Offset, true

	return changes, nil
}

// completeUpdate fills a partial update from the stored detection it changes.
func (e *Engine) completeUpdate(frame Frame) (Frame, error) {
	if frame.Type != "update" || frame.Changes == nil {
		return frame, nil
	}

	previous, exists := e.state.Records[frame.Key]
	if !exists || frame.Detection != nil {
		return frame, errors.New("partial update requires a stored detection")
	}

	detection := previous.Detection
	if frame.Changes.Confidence != nil {
		detection.Confidence = *frame.Changes.Confidence
	}

	if frame.Changes.FRP != nil {
		detection.FRP = *frame.Changes.FRP
	}

	frame.Detection = &detection

	return frame, nil
}

// frameKey validates a frame and returns the detection key it concerns. A heartbeat has none.
func frameKey(frame Frame) (string, error) {
	switch frame.Type {
	case "detection", "update":
		if frame.Detection == nil || frame.Detection.Validate() != nil {
			return "", errors.New("invalid detection frame")
		}

		computed := frame.Detection.Key()
		if frame.Key != "" && frame.Key != computed {
			return "", errors.New("detection identity changed")
		}

		return computed, nil
	case "retract":
		if frame.Key == "" || len(frame.Key) > 512 {
			return "", errors.New("retraction requires detection key")
		}

		return frame.Key, nil
	case "heartbeat":
		return "", nil
	}

	return "", errors.New("unknown stream event")
}

// record stores a detection. A detection older than the watermark minus AllowedLate is kept
// as late and joins no fire, and a low-confidence detection joins no fire either.
func (e *Engine) record(frame Frame, key string, old Record, exists bool, affected map[string]bool) {
	d := *frame.Detection
	when, _ := d.Time()

	late := exists && old.Late || !exists && !e.state.Watermark.IsZero() && when.Before(e.state.Watermark.Add(-AllowedLate))
	if when.After(e.state.Watermark) {
		e.state.Watermark = when
	}

	e.state.Records[key] = Record{Detection: d, Late: late}
	delete(e.state.Tombstones, key)

	if late {
		frame.Detection = &d
		frame.Raw = append(json.RawMessage(nil), frame.Raw...)
		e.state.Late = append(e.state.Late, LateRecord{Frame: frame, SeenAt: e.state.Watermark})
	} else if d.Confidence != "low" {
		e.add(key)
		e.component(key, affected)
	}
}

func (e *Engine) earlier(a, b string) bool {
	ta, _ := e.state.Records[a].Detection.Time()

	tb, _ := e.state.Records[b].Detection.Time()
	if !ta.Equal(tb) {
		return ta.Before(tb)
	}

	return a < b
}

func (e *Engine) reconcile(affected map[string]bool, offset int64) []Fire {
	old := e.previousFires(affected)
	components := e.components(affected)
	preferred := e.preferredComponents(components, old)

	assigned := map[string]bool{}
	winners := map[int]string{}
	changes := map[string]Fire{}

	for i, keys := range components {
		fire := e.summarize(e.chooseFire(i, keys, preferred, old, offset), keys)

		assigned[fire.ID], winners[i] = true, fire.ID
		if previous, exists := e.state.Fires[fire.ID]; !exists || !equalFire(previous, fire) {
			changes[fire.ID] = fire
		}

		e.state.Fires[fire.ID] = fire
		for _, key := range keys {
			e.state.Owners[key] = fire.ID
		}
	}

	e.retireFires(old, assigned, preferred, winners, changes)

	out := make([]Fire, 0, len(changes))
	for _, fire := range changes {
		out = append(out, fire)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// previousFires returns the live fires that own an affected key.
func (e *Engine) previousFires(affected map[string]bool) map[string]Fire {
	old := map[string]Fire{}

	for key := range affected {
		id := e.state.Owners[key]

		fire, exists := e.state.Fires[id]
		if !exists || fire.Status == "merged" {
			continue
		}
		// A singleton may remember an ID retained by another split component.
		// It cannot take that active identity away from the disconnected fire.
		if fire.Status == "active" && !contains(fire.Keys, key) {
			continue
		}

		old[id] = fire
	}

	return old
}

// components returns the groups of two or more connected keys, each sorted by detection time.
func (e *Engine) components(affected map[string]bool) [][]string {
	components := [][]string{}
	visited := map[string]bool{}

	for key := range affected {
		if visited[key] {
			continue
		}

		members := map[string]bool{}
		e.component(key, members)

		keys := make([]string, 0, len(members))
		for member := range members {
			visited[member] = true
			keys = append(keys, member)
		}

		if len(keys) >= 2 {
			sort.Slice(keys, func(i, j int) bool { return e.earlier(keys[i], keys[j]) })
			components = append(components, keys)
		}
	}

	sort.Slice(components, func(i, j int) bool { return e.earlier(components[i][0], components[j][0]) })

	return components
}

// preferredComponents gives each old fire the largest component it still holds keys in,
// and the earliest one on a tie.
func (e *Engine) preferredComponents(components [][]string, old map[string]Fire) map[string]int {
	preferred := map[string]int{}

	for i, keys := range components {
		for _, key := range keys {
			id := e.state.Owners[key]
			if _, exists := old[id]; !exists {
				continue
			}

			j, chosen := preferred[id]
			if !chosen || len(keys) > len(components[j]) || len(keys) == len(components[j]) && e.earlier(keys[0], components[j][0]) {
				preferred[id] = i
			}
		}
	}

	return preferred
}

// chooseFire keeps the identity of the largest old fire that prefers this component, and
// mints a new identity when none does.
func (e *Engine) chooseFire(i int, keys []string, preferred map[string]int, old map[string]Fire, offset int64) Fire {
	candidates := []Fire{}

	for id, index := range preferred {
		if index == i {
			candidates = append(candidates, old[id])
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}

		if a.MintOffset != b.MintOffset {
			return a.MintOffset < b.MintOffset
		}

		return a.ID < b.ID
	})

	if len(candidates) > 0 {
		return candidates[0]
	}

	id := identity(keys[0])
	if _, exists := e.state.Fires[id]; exists {
		id += "-" + strconv.FormatInt(offset, 16)
	}

	return Fire{ID: id, MintOffset: offset}
}

// retireFires marks each old fire that kept no component as merged into the fire of its
// preferred component, or as withdrawn when it has none.
func (e *Engine) retireFires(old map[string]Fire, assigned map[string]bool, preferred map[string]int, winners map[int]string, changes map[string]Fire) {
	for id, fire := range old {
		if assigned[id] {
			continue
		}

		fire.Status, fire.MergedInto = "withdrawn", ""
		if index, exists := preferred[id]; exists {
			fire.Status, fire.MergedInto = "merged", winners[index]
		}

		fire.Keys, fire.FeedKeys, fire.Satellites = []string{}, nil, []string{}

		fire.Count, fire.TotalFRP, fire.HighestConfidence, fire.Severity = 0, 0, "", "NORMAL"
		if !equalFire(e.state.Fires[id], fire) {
			changes[id] = fire
		}

		e.state.Fires[id] = fire
	}
}

func contains(keys []string, wanted string) bool {
	for _, key := range keys {
		if key == wanted {
			return true
		}
	}

	return false
}

func equalFire(a, b Fire) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)

	return bytes.Equal(x, y)
}

func (e *Engine) summarize(fire Fire, chronological []string) Fire {
	fire.Status, fire.MergedInto = "active", ""

	fire.Keys = append([]string(nil), chronological...)
	sort.Strings(fire.Keys)
	fire.Count, fire.TotalFRP, fire.Latitude, fire.Longitude = len(fire.Keys), 0, 0, 0
	fire.OccurredAt, _ = e.state.Records[chronological[0]].Detection.Time()
	fire.HighestConfidence = "nominal"
	satellites := map[string]bool{}
	// Key order fixes floating-point addition order across restarts and map walks.
	for _, key := range fire.Keys {
		d := e.state.Records[key].Detection
		fire.TotalFRP += d.FRP
		fire.Latitude += d.Latitude
		fire.Longitude += d.Longitude

		satellites[d.Satellite] = true
		if d.Confidence == "high" {
			fire.HighestConfidence = "high"
		}
	}

	fire.Latitude /= float64(fire.Count)
	fire.Longitude /= float64(fire.Count)

	fire.FeedKeys = make([]string, 0, len(fire.Keys))
	for _, key := range fire.Keys {
		fire.FeedKeys = append(fire.FeedKeys, e.state.Records[key].Detection.ReportKey())
	}

	sort.Strings(fire.FeedKeys)

	fire.Satellites = []string{}
	for satellite := range satellites {
		fire.Satellites = append(fire.Satellites, satellite)
	}

	sort.Strings(fire.Satellites)
	fire.Severity = Severity(fire.Count, fire.TotalFRP)

	return fire
}

func (e *Engine) Stats() Stats {
	s := Stats{Offset: e.state.Offset, Detections: len(e.edges), Singletons: len(e.edges), Tombstones: len(e.state.Tombstones)}
	late := map[string]bool{}

	for _, record := range e.state.Late {
		if record.Frame.Detection != nil {
			late[record.Frame.Detection.Key()] = true
		}
	}

	s.LateCount = int64(len(late))

	for _, fire := range e.state.Fires {
		if fire.Status == "active" {
			s.Active++
			s.Singletons -= fire.Count
		}
	}

	return s
}

func (e *Engine) Fires() []Fire {
	out := make([]Fire, 0, len(e.state.Fires))
	for _, fire := range e.state.Fires {
		// Return detached slices so callers cannot mutate the durable state.
		fire.Keys = append([]string{}, fire.Keys...)
		fire.Satellites = append([]string{}, fire.Satellites...)
		out = append(out, fire)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

func (e *Engine) Check() error {
	seen := map[string]bool{}

	for id, fire := range e.state.Fires {
		if id != fire.ID || fire.Count != len(fire.Keys) || fire.Status != "active" && fire.Count != 0 {
			return errors.New("invalid fire checkpoint")
		}

		for _, key := range fire.Keys {
			if seen[key] || e.edges[key] == nil || e.state.Owners[key] != id {
				return errors.New("detection belongs to multiple or invalid fires")
			}

			seen[key] = true
		}
	}

	if e.Stats().Singletons < 0 {
		return errors.New("invalid singleton count")
	}

	return nil
}
