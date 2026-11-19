package satellite

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"sort"
	"strings"
)

type Province struct {
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Reference struct {
	Version      string     `json:"version"`
	Source       string     `json:"source,omitempty"`
	SourceSHA256 string     `json:"source_sha256,omitempty"`
	License      string     `json:"license,omitempty"`
	Attribution  string     `json:"attribution,omitempty"`
	Provinces    []Province `json:"provinces"`
}

//go:embed reference/provinces.json
var defaultReference []byte

func LoadReference(path string) (Reference, error) {
	raw := defaultReference

	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			return Reference{}, errors.New("province reference unavailable")
		}
		defer file.Close()

		raw, err = io.ReadAll(io.LimitReader(file, (1<<20)+1))
		if err != nil || len(raw) > 1<<20 {
			return Reference{}, errors.New("province reference exceeds boundary")
		}
	}

	var reference Reference

	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()

	var extra any
	if d.Decode(&reference) != nil || d.Decode(&extra) != io.EOF || reference.Version == "" || len(reference.Provinces) == 0 {
		return reference, errors.New("invalid province reference")
	}

	seen := map[string]bool{}
	for _, p := range reference.Provinces {
		if strings.TrimSpace(p.Name) == "" || seen[p.Name] || math.IsNaN(p.Latitude) || math.IsNaN(p.Longitude) || math.Abs(p.Latitude) > 90 || math.Abs(p.Longitude) > 180 {
			return reference, errors.New("invalid province centre")
		}

		seen[p.Name] = true
	}

	sort.Slice(reference.Provinces, func(i, j int) bool { return reference.Provinces[i].Name < reference.Provinces[j].Name })

	return reference, nil
}

func (r Reference) Nearest(lat, lon float64) string {
	name, best := "", math.Inf(1)

	for _, p := range r.Provinces {
		distance := DistanceKM(lat, lon, p.Latitude, p.Longitude)
		if distance < best || distance == best && p.Name < name {
			name, best = p.Name, distance
		}
	}

	return name
}
