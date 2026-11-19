package satellite

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSSECommentsMultilineAndBoundaryValidation(t *testing.T) {
	body := ": heartbeat\r\n\r\nevent: detection\r\nid: 1\r\ndata: {\r\ndata: \"satellite\":\"N20\",\"acq_date\":\"2026-11-18\",\"acq_time\":15,\"latitude\":-6,\"longitude\":106,\"confidence\":\"n\",\"frp\":4}\r\n\r\nevent: update\nid: 2\ndata: {\"unfinished\":"

	var frames []Frame

	_ = ReadSSE(context.Background(), strings.NewReader(body), func(event SSEEvent) error {
		frame, err := DecodeFeed(event)
		frames = append(frames, frame)

		return err
	})

	if len(frames) != 1 || frames[0].Detection == nil || frames[0].Detection.AcqTime != "0015" || frames[0].Detection.Confidence != "nominal" {
		t.Fatal("SSE framing or explicit wire normalization", frames)
	}

	for _, raw := range [][]byte{[]byte("event: detection\nid: -1\ndata: {}\n\n"), []byte("data: " + strings.Repeat("x", MaxFrameBytes+1) + "\n\n"), {'d', 'a', 't', 'a', ':', ' ', 0xff, '\n', '\n'}} {
		if err := ReadSSE(context.Background(), bytes.NewReader(raw), func(SSEEvent) error { return nil }); err == nil {
			t.Fatal("unsafe SSE boundary accepted")
		}
	}
}

func TestFeedDecoderRejectsMissingIdentityAndInvalidDepth(t *testing.T) {
	d := detection(106)

	raw, _ := json.Marshal(d)
	for _, body := range []string{strings.Replace(string(raw), `"latitude":-6,`, "", 1), `{"extra":` + strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33) + "}", `{"satellite":"N20","acq_date":"2026-11-18","acq_time":1260,"latitude":0,"longitude":0,"confidence":"n","frp":4}`} {
		if _, err := DecodeFeed(SSEEvent{Type: "detection", Offset: 1, Data: []byte(body)}); err == nil {
			t.Fatal("invalid feed payload accepted", body)
		}
	}
}

func TestReferenceUsesNearestCentreAndStableTies(t *testing.T) {
	ref, err := LoadReference("")
	if err != nil || len(ref.Provinces) != 34 || ref.Version == "" {
		t.Fatal("versioned fallback reference", err)
	}

	for _, p := range ref.Provinces {
		if got := ref.Nearest(p.Latitude, p.Longitude); got != p.Name {
			t.Fatal("centre does not select its own province", p.Name, got)
		}
	}

	ref = Reference{Provinces: []Province{{Name: "B", Latitude: 0, Longitude: 1}, {Name: "A", Latitude: 0, Longitude: -1}}}
	if got := ref.Nearest(0, 0); got != "A" {
		t.Fatal("distance tie depends on table order", got)
	}
}
