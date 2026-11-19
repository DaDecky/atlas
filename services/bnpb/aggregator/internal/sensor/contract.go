// Package sensor reads the Sensor Pantai through the contract of Lampiran C.
package sensor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

type Station struct {
	ID, Name, Province  string
	Latitude, Longitude float64
}

type Observation struct {
	ID, StationID               string
	ObservedAt                  time.Time
	SeaLevelAnomaly, WaveHeight float64
	Quality                     string
	Critical                    bool
}

// Item is one record of a page. Violation is set when the record breaks the contract.
type Item struct {
	Raw         json.RawMessage
	Observation Observation
	Violation   string
}

type Page struct {
	GeneratedAt time.Time
	Items       []Item
	NextCursor  string
}

// ErrContract marks a response body that breaks the contract as a whole.
var ErrContract = errors.New("sensor response breaks the contract")

func DecodePage(body []byte) (Page, error) {
	var raw struct {
		GeneratedAt *string           `json:"generated_at"`
		Items       []json.RawMessage `json:"items"`
		NextCursor  *string           `json:"next_cursor"`
	}

	if err := strictUnmarshal(body, &raw); err != nil {
		return Page{}, fmt.Errorf("%w: %v", ErrContract, err)
	}

	if raw.GeneratedAt == nil || raw.Items == nil || raw.NextCursor == nil {
		return Page{}, fmt.Errorf("%w: generated_at, items, and next_cursor are required", ErrContract)
	}

	generated, err := time.Parse(time.RFC3339, *raw.GeneratedAt)
	if err != nil {
		return Page{}, fmt.Errorf("%w: generated_at is not RFC 3339", ErrContract)
	}

	page := Page{GeneratedAt: generated, NextCursor: *raw.NextCursor, Items: make([]Item, 0, len(raw.Items))}
	for _, record := range raw.Items {
		observation, err := decodeObservation(record, generated)

		item := Item{Raw: record, Observation: observation}
		if err != nil {
			item.Violation = err.Error()
		}

		page.Items = append(page.Items, item)
	}

	return page, nil
}

func decodeObservation(record json.RawMessage, generated time.Time) (Observation, error) {
	var raw struct {
		ID         *string  `json:"observation_id"`
		StationID  *string  `json:"station_id"`
		ObservedAt *string  `json:"observed_at"`
		Anomaly    *float64 `json:"sea_level_anomaly_m"`
		Wave       *float64 `json:"wave_height_m"`
		Quality    *string  `json:"quality"`
		Critical   *bool    `json:"critical"`
	}

	if err := strictUnmarshal(record, &raw); err != nil {
		return Observation{}, fmt.Errorf("record is not a valid observation: %w", err)
	}

	if raw.ID == nil || raw.StationID == nil || raw.ObservedAt == nil || raw.Anomaly == nil || raw.Wave == nil || raw.Quality == nil || raw.Critical == nil {
		return Observation{}, errors.New("a required field is missing or null")
	}

	o := Observation{ID: *raw.ID, StationID: *raw.StationID, SeaLevelAnomaly: *raw.Anomaly, WaveHeight: *raw.Wave, Quality: *raw.Quality, Critical: *raw.Critical}
	if o.ID == "" || o.StationID == "" || o.Quality == "" {
		return o, errors.New("observation_id, station_id, and quality must not be empty")
	}

	observed, err := time.Parse(time.RFC3339, *raw.ObservedAt)
	if err != nil {
		return o, errors.New("observed_at is not RFC 3339")
	}

	o.ObservedAt = observed
	if observed.After(generated) {
		return o, errors.New("observed_at is later than generated_at")
	}

	if o.WaveHeight < 0 || math.IsInf(o.WaveHeight, 0) || math.IsInf(o.SeaLevelAnomaly, 0) {
		return o, errors.New("wave height is negative or a value is not finite")
	}

	return o, nil
}

func DecodeStations(body []byte) (map[string]Station, error) {
	var raw struct {
		Items []struct {
			ID        *string  `json:"station_id"`
			Name      *string  `json:"name"`
			Province  *string  `json:"province"`
			Latitude  *float64 `json:"latitude"`
			Longitude *float64 `json:"longitude"`
		} `json:"items"`
	}

	if err := strictUnmarshal(body, &raw); err != nil || raw.Items == nil {
		return nil, fmt.Errorf("%w: stations body is invalid", ErrContract)
	}

	stations := map[string]Station{}

	for _, s := range raw.Items {
		if s.ID == nil || *s.ID == "" || s.Name == nil || s.Province == nil || s.Latitude == nil || s.Longitude == nil ||
			*s.Latitude < -90 || *s.Latitude > 90 || *s.Longitude < -180 || *s.Longitude > 180 {
			return nil, fmt.Errorf("%w: station record is invalid", ErrContract)
		}

		stations[*s.ID] = Station{ID: *s.ID, Name: *s.Name, Province: *s.Province, Latitude: *s.Latitude, Longitude: *s.Longitude}
	}

	return stations, nil
}

// strictUnmarshal rejects trailing data, so a body with two documents is not read as its first.
func strictUnmarshal(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(target); err != nil {
		return err
	}

	if decoder.More() {
		return errors.New("trailing data after the JSON value")
	}

	return nil
}
