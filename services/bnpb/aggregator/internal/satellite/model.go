// Package satellite processes ordered hotspot corrections without wall clocks.
package satellite

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

type Detection struct {
	Satellite  string  `json:"satellite"`
	AcqDate    string  `json:"acq_date"`
	AcqTime    string  `json:"acq_time"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Confidence string  `json:"confidence"`
	FRP        float64 `json:"frp"`
}

type Frame struct {
	Offset    int64           `json:"offset"`
	Type      string          `json:"type"`
	Detection *Detection      `json:"detection,omitempty"`
	Key       string          `json:"key,omitempty"`
	Raw       json.RawMessage `json:"raw,omitempty"`
}

type Record struct {
	Detection Detection `json:"detection"`
	Retracted bool      `json:"retracted"`
	Late      bool      `json:"late"`
}

type LateRecord struct {
	Frame  Frame     `json:"frame"`
	SeenAt time.Time `json:"watermark"`
}

type Fire struct {
	ID                string    `json:"fire_id"`
	MintOffset        int64     `json:"mint_offset"`
	Status            string    `json:"status"`
	MergedInto        string    `json:"merged_into,omitempty"`
	Keys              []string  `json:"detection_keys"`
	Count             int       `json:"detection_count"`
	Satellites        []string  `json:"satellites"`
	HighestConfidence string    `json:"highest_confidence"`
	TotalFRP          float64   `json:"total_frp_mw"`
	Latitude          float64   `json:"latitude"`
	Longitude         float64   `json:"longitude"`
	OccurredAt        time.Time `json:"occurred_at"`
	Severity          string    `json:"severity"`
}

type State struct {
	Version    int               `json:"version"`
	HasOffset  bool              `json:"has_offset"`
	Offset     int64             `json:"offset"`
	Watermark  time.Time         `json:"watermark"`
	Records    map[string]Record `json:"records"`
	Tombstones map[string]bool   `json:"tombstones"`
	Owners     map[string]string `json:"owners"`
	Fires      map[string]Fire   `json:"fires"`
	Late       []LateRecord      `json:"late"`
}

const (
	EarthRadiusKM = 6371.0088
	RadiusKM      = 1.0
	Window        = 360 * time.Minute
	AllowedLate   = 360 * time.Minute
)

func (d Detection) Time() (time.Time, error) {
	if len(d.AcqDate) != 10 || len(d.AcqTime) != 4 {
		return time.Time{}, errors.New("detection time requires YYYY-MM-DD and HHMM UTC")
	}

	return time.Parse("2006-01-02 1504", d.AcqDate+" "+d.AcqTime)
}

func (d Detection) Validate() error {
	if _, err := d.Time(); err != nil {
		return err
	}

	if strings.TrimSpace(d.Satellite) != d.Satellite || d.Satellite == "" || len(d.Satellite) > 64 || strings.ContainsRune(d.Satellite, 0) ||
		math.IsNaN(d.Latitude) || math.IsInf(d.Latitude, 0) || math.Abs(d.Latitude) > 90 ||
		math.IsNaN(d.Longitude) || math.IsInf(d.Longitude, 0) || math.Abs(d.Longitude) > 180 ||
		math.IsNaN(d.FRP) || math.IsInf(d.FRP, 0) || d.FRP < 0 {
		return errors.New("invalid detection fields")
	}

	switch d.Confidence {
	case "low", "nominal", "high":
		return nil
	default:
		return errors.New("unknown confidence")
	}
}

func coordinate(v float64) string {
	if v == 0 {
		return "0"
	}

	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Key is an unambiguous, locale-independent representation of the five identity
// fields. Numeric coordinate formatting, including signed zero, is normalized.
func (d Detection) Key() string {
	raw, _ := json.Marshal([5]string{d.Satellite, d.AcqDate, d.AcqTime, coordinate(d.Latitude), coordinate(d.Longitude)})
	return string(raw)
}

func DistanceKM(lat1, lon1, lat2, lon2 float64) float64 {
	rad := math.Pi / 180
	dLat, dLon := (lat2-lat1)*rad, (lon2-lon1)*rad
	a := math.Pow(math.Sin(dLat/2), 2) + math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Pow(math.Sin(dLon/2), 2)

	return 2 * EarthRadiusKM * math.Asin(math.Sqrt(min(1, max(0, a))))
}

func Connected(a, b Detection) bool {
	ta, _ := a.Time()
	tb, _ := b.Time()
	delta := ta.Sub(tb)

	return delta >= -Window && delta <= Window && DistanceKM(a.Latitude, a.Longitude, b.Latitude, b.Longitude) <= RadiusKM
}

func Severity(count int, frp float64) string {
	if count >= 20 || frp >= 100 {
		return "AWAS"
	}

	if count >= 6 || frp >= 30 {
		return "SIAGA"
	}

	return "WASPADA"
}

func identity(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "fire-" + hex.EncodeToString(sum[:6])
}
