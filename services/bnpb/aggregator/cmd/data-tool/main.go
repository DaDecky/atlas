// data-tool loads canonical JSONL or generates reproducible regional spikes.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"atlas/services/bnpb/aggregator/internal/placement"

	"atlas/services/bnpb/aggregator/internal/aggregator"
)

func run() error {
	mode := flag.String("mode", "generate", "generate, placement, export-m1, load, or metrics")
	nodeNames := flag.String("nodes", "", "comma-separated Kubernetes node names for placement")
	namespace := flag.String("namespace", "sistem-informasi-kebencanaan-bnpb", "placement namespace")
	placementName := flag.String("placement-name", "canonical-placement", "placement ConfigMap name")
	count := flag.Int("count", 1000, "synthetic event count")
	prefix := flag.String("prefix", "synthetic-quake", "unique reference prefix for this dataset or spike")
	region := flag.String("region", "Jawa Barat", "region for the regional spike")
	file := flag.String("input", "-", "canonical HazardEvent JSONL input, - for stdin")
	config := flag.String("config", "", "mounted M2 data configuration")
	epoch := flag.Int64("epoch", 1, "currently sealed BMKG writer epoch")

	flag.Parse()

	if *mode == "placement" {
		table, err := placement.New(strings.Split(*nodeNames, ","))
		if err != nil {
			return err
		}

		for _, source := range []string{"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"} {
			table.WriterEpochs[source] = 1
		}

		raw, err := json.Marshal(table)
		if err != nil {
			return err
		}

		return json.NewEncoder(os.Stdout).Encode(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]string{"name": *placementName, "namespace": *namespace}, "data": map[string]string{"placement.json": string(raw)}})
	}

	if *mode == "export-m1" {
		url := os.Getenv("DATABASE_URL")
		if url == "" {
			return fmt.Errorf("DATABASE_URL required for export-m1")
		}

		ctx := context.Background()

		store, err := aggregator.OpenStore(ctx, url, 2, 4)
		if err != nil {
			return err
		}
		defer store.Close()

		encoder := json.NewEncoder(os.Stdout)

		query := aggregator.Query{Limit: 1000, Audit: true}
		for {
			page, err := store.Read(ctx, query)
			if err != nil {
				return err
			}

			for _, event := range page.Data {
				if err = encoder.Encode(event); err != nil {
					return err
				}
			}

			if page.NextCursor == nil {
				break
			}

			query.Cursor = *page.NextCursor
		}

		return nil
	}

	if *count < 1 {
		return fmt.Errorf("count must be positive")
	}

	if *mode == "generate" {
		encoder := json.NewEncoder(os.Stdout)
		base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

		for i := 0; i < *count; i++ {
			ref := fmt.Sprintf("%s-%08d", *prefix, i)

			event := aggregator.HazardEvent{HazardID: aggregator.CanonicalHazardID("BMKG", ref), Source: "BMKG", SourceRefID: ref, HazardType: "SEISMIC", Severity: "WASPADA", AreaName: *region, Latitude: -6.9 + float64(i%100)/10000, Longitude: 107.6, OccurredAt: base.Add(time.Duration(i) * time.Second), IngestedAt: base.Add(time.Duration(i) * time.Millisecond), Attributes: map[string]json.RawMessage{"magnitude": json.RawMessage(`5.1`)}}
			if err := encoder.Encode(event); err != nil {
				return err
			}
		}

		return nil
	}

	if (*mode != "load" && *mode != "metrics") || *config == "" {
		return fmt.Errorf("load requires -config; input is canonical JSONL, convert assistant CSV explicitly before loading")
	}

	ctx := context.Background()

	store, err := aggregator.OpenClusterConfig(ctx, *config)
	if err != nil {
		return err
	}
	defer store.Close()

	if *mode == "metrics" {
		metrics, err := store.NodeMetrics(ctx)
		if err != nil {
			return err
		}

		return json.NewEncoder(os.Stdout).Encode(metrics)
	}

	var input io.Reader = os.Stdin

	if *file != "-" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		defer f.Close()

		input = f
	}

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)

	batch := aggregator.WriteBatch{Epoch: *epoch}
	loaded := 0
	start := time.Now()
	flush := func() error {
		if len(batch.Events) == 0 {
			return nil
		}

		batch.Now = time.Now().UTC()

		_, err := store.Upsert(ctx, batch)
		if err == nil {
			loaded += len(batch.Events)
			batch.Events = nil
		}

		return err
	}

	for scanner.Scan() {
		var h aggregator.HazardEvent
		if err = json.Unmarshal(scanner.Bytes(), &h); err != nil {
			return fmt.Errorf("line %d: %w", loaded+len(batch.Events)+1, err)
		}

		if batch.Source != "" && batch.Source != h.Source {
			if err = flush(); err != nil {
				return err
			}
		}

		batch.Source = h.Source

		batch.Events = append(batch.Events, h)
		if len(batch.Events) == 100 {
			if err = flush(); err != nil {
				return err
			}
		}
	}

	if err = scanner.Err(); err != nil {
		return err
	}

	if err = flush(); err != nil {
		return err
	}

	metrics, err := store.NodeMetrics(ctx)
	if err != nil {
		return err
	}

	return json.NewEncoder(os.Stdout).Encode(map[string]any{"loaded": loaded, "seconds": time.Since(start).Seconds(), "nodes": metrics})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
