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

	"atlas/services/bnpb/aggregator/internal/aggregator"
	"atlas/services/bnpb/aggregator/internal/placement"
)

type options struct {
	mode, nodes, namespace, placementName, prefix, region, input, config string
	count                                                                int
	epoch                                                                int64
}

func run() error {
	var o options

	flag.StringVar(&o.mode, "mode", "generate", "generate, placement, export-store, load, or metrics")
	flag.StringVar(&o.nodes, "nodes", "", "comma-separated Kubernetes node names for placement")
	flag.StringVar(&o.namespace, "namespace", "sistem-informasi-kebencanaan-bnpb", "placement namespace")
	flag.StringVar(&o.placementName, "placement-name", "canonical-placement", "placement ConfigMap name")
	flag.IntVar(&o.count, "count", 1000, "synthetic event count")
	flag.StringVar(&o.prefix, "prefix", "synthetic-quake", "unique reference prefix for this dataset or spike")
	flag.StringVar(&o.region, "region", "Jawa Barat", "region for the regional spike")
	flag.StringVar(&o.input, "input", "-", "canonical HazardEvent JSONL input, - for stdin")
	flag.StringVar(&o.config, "config", "", "mounted cluster data configuration")
	flag.Int64Var(&o.epoch, "epoch", 1, "currently sealed BMKG writer epoch")
	flag.Parse()

	switch o.mode {
	case "placement":
		return printPlacement(o)
	case "export-store":
		return exportStore()
	}

	if o.count < 1 {
		return fmt.Errorf("count must be positive")
	}

	if o.mode == "generate" {
		return generate(o)
	}

	if (o.mode != "load" && o.mode != "metrics") || o.config == "" {
		return fmt.Errorf("load requires -config; input is canonical JSONL, convert assistant CSV explicitly before loading")
	}

	return loadOrMeasure(o)
}

// printPlacement writes the initial placement ConfigMap with every source at writer epoch 1.
func printPlacement(o options) error {
	table, err := placement.New(strings.Split(o.nodes, ","))
	if err != nil {
		return err
	}

	for _, source := range aggregator.Sources {
		table.WriterEpochs[source] = 1
	}

	raw, err := json.Marshal(table)
	if err != nil {
		return err
	}

	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]string{"name": o.placementName, "namespace": o.namespace},
		"data":     map[string]string{"placement.json": string(raw)},
	})
}

// exportStore writes every event of a single-instance store as canonical JSONL.
func exportStore() error {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL required for export-store")
	}

	ctx := context.Background()

	store, err := aggregator.OpenStore(ctx, url, 2, 4, 2*time.Second)
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
			return nil
		}

		query.Cursor = *page.NextCursor
	}
}

// generate writes a reproducible regional spike of BMKG events as canonical JSONL.
func generate(o options) error {
	encoder := json.NewEncoder(os.Stdout)
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	for i := range o.count {
		ref := fmt.Sprintf("%s-%08d", o.prefix, i)
		event := aggregator.HazardEvent{
			HazardID: aggregator.CanonicalHazardID(aggregator.SourceBMKG, ref), Source: aggregator.SourceBMKG, SourceRefID: ref,
			HazardType: "SEISMIC", Severity: aggregator.SeverityWaspada, AreaName: o.region,
			Latitude: -6.9 + float64(i%100)/10000, Longitude: 107.6,
			OccurredAt: base.Add(time.Duration(i) * time.Second), IngestedAt: base.Add(time.Duration(i) * time.Millisecond),
			Attributes: map[string]json.RawMessage{"magnitude": json.RawMessage(`5.1`)},
		}

		if err := encoder.Encode(event); err != nil {
			return err
		}
	}

	return nil
}

func loadOrMeasure(o options) error {
	ctx := context.Background()

	store, err := aggregator.OpenClusterConfig(ctx, o.config, aggregator.DefaultClusterLimits())
	if err != nil {
		return err
	}
	defer store.Close()

	if o.mode == "metrics" {
		metrics, err := store.NodeMetrics(ctx)
		if err != nil {
			return err
		}

		return json.NewEncoder(os.Stdout).Encode(metrics)
	}

	var input io.Reader = os.Stdin

	if o.input != "-" {
		f, err := os.Open(o.input)
		if err != nil {
			return err
		}
		defer f.Close()

		input = f
	}

	start := time.Now()

	loaded, err := load(ctx, store, input, o.epoch)
	if err != nil {
		return err
	}

	metrics, err := store.NodeMetrics(ctx)
	if err != nil {
		return err
	}

	return json.NewEncoder(os.Stdout).Encode(map[string]any{"loaded": loaded, "seconds": time.Since(start).Seconds(), "nodes": metrics})
}

// load writes canonical JSONL in batches of 100 events of one source.
func load(ctx context.Context, store *aggregator.Cluster, input io.Reader, epoch int64) (int, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)

	batch := aggregator.WriteBatch{Epoch: epoch}
	loaded := 0

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
		if err := json.Unmarshal(scanner.Bytes(), &h); err != nil {
			return loaded, fmt.Errorf("line %d: %w", loaded+len(batch.Events)+1, err)
		}

		if batch.Source != "" && batch.Source != h.Source {
			if err := flush(); err != nil {
				return loaded, err
			}
		}

		batch.Source = h.Source

		batch.Events = append(batch.Events, h)
		if len(batch.Events) == 100 {
			if err := flush(); err != nil {
				return loaded, err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return loaded, err
	}

	return loaded, flush()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
