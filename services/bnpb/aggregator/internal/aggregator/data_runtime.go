package aggregator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"atlas/services/bnpb/aggregator/internal/placement"
)

type DataConfig struct {
	Nodes []struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"nodes"`
	Placement struct{ Origin, Namespace, Name, TokenFile, CAFile string } `json:"placement"`
}

// Save is a compatibility bridge for M1 pollers. Multi-instance production
// callers must use EpochIngest after acquiring/renewing real source leadership.
type EpochIngest struct {
	Data        ShardStore
	Source      string
	WriterEpoch int64
}

// OpenClusterConfig reads mounted configuration containing node DSNs and API
// connection details. Keep it in a Secret, never in a committed ConfigMap.
func OpenClusterConfig(ctx context.Context, path string) (*Cluster, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config DataConfig
	if err = json.Unmarshal(raw, &config); err != nil {
		return nil, err
	}

	if len(config.Nodes) < 3 {
		return nil, errors.New("at least three configured data nodes required")
	}

	client := &http.Client{Timeout: 5 * time.Second}

	token, err := os.ReadFile(config.Placement.TokenFile)
	if err != nil {
		return nil, err
	}

	if config.Placement.CAFile != "" {
		raw, err := os.ReadFile(config.Placement.CAFile)
		if err != nil {
			return nil, err
		}

		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(raw) {
			return nil, errors.New("invalid placement CA")
		}

		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	}

	c := &Cluster{Nodes: map[string]*DataNode{}, Deadline: 2 * time.Second, Placement: &placement.ConfigMap{Client: client, Origin: config.Placement.Origin, Namespace: config.Placement.Namespace, Name: config.Placement.Name, Token: string(token)}}
	for _, entry := range config.Nodes {
		if entry.Name == "" || c.Nodes[entry.Name] != nil {
			c.Close()
			return nil, errors.New("invalid data node names")
		}

		node, err := OpenDataNode(ctx, entry.Name, entry.URL)
		if err != nil {
			node, err = offlineDataNode(entry.Name, entry.URL)
			if err != nil {
				c.Close()
				return nil, err
			}
		}

		c.Nodes[entry.Name] = node
	}

	if _, _, err = c.Placement.Load(ctx); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func (c *Cluster) Close() {
	for _, n := range c.Nodes {
		n.Store.Close()
	}
}

func (e *EpochIngest) Checkpoint(ctx context.Context, source string) (*time.Time, error) {
	return e.Data.Checkpoint(ctx, source)
}

func (e *EpochIngest) Save(ctx context.Context, source string, events []HazardEvent, checkpoint *time.Time, now time.Time) ([]HazardEvent, error) {
	if source != e.Source {
		return nil, errors.New("writer is bound to a different source")
	}

	b := WriteBatch{Source: source, Epoch: e.WriterEpoch, Events: events, Now: now}
	if checkpoint != nil {
		return e.Data.CommitCheckpoint(ctx, b, *checkpoint)
	}

	return e.Data.Upsert(ctx, b)
}

func (e *EpochIngest) Failure(ctx context.Context, source string, now time.Time) error {
	switch data := e.Data.(type) {
	case *Store:
		return data.Failure(ctx, source, now)
	case *Cluster:
		return data.Failure(ctx, source, now)
	}

	return errors.New("unsupported freshness store")
}

func (e *EpochIngest) WarningEvents(ctx context.Context, refs []string) ([]HazardEvent, error) {
	switch data := e.Data.(type) {
	case *Store:
		return data.WarningEvents(ctx, refs)
	case *Cluster:
		return data.WarningEvents(ctx, refs)
	}

	return nil, errors.New("unsupported warning store")
}

func (c *Cluster) Failure(ctx context.Context, source string, now time.Time) error {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	for _, copy := range t.Shards[placement.Shard("metadata:"+source)] {
		if copy.State == "behind" || c.Nodes[copy.Node] == nil {
			continue
		}

		s := c.Nodes[copy.Node].Shards[placement.Shard("metadata:"+source)]

		tx, err := s.begin(ctx, false)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `UPDATE source_progress SET stale_since=COALESCE(stale_since,$2) WHERE source=$1`, source, now)
		if err == nil {
			err = tx.Commit(ctx)
		}

		_ = tx.Rollback(ctx)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Cluster) WarningEvents(ctx context.Context, refs []string) ([]HazardEvent, error) {
	wanted := map[string]bool{}
	for _, ref := range refs {
		wanted[ref] = true
	}

	events, missing, err := c.fanout(ctx, func(ctx context.Context, s *Store) ([]HazardEvent, error) {
		tx, err := s.begin(ctx, true)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(ctx)

		rows, err := tx.Query(ctx, `SELECT `+columns+` FROM hazard_events WHERE source='BMKG' AND source_ref_id=ANY($1)`, refs)
		if err != nil {
			return nil, err
		}

		out := []HazardEvent{}

		for rows.Next() {
			h, err := scanEvent(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}

			if wanted[h.SourceRefID] {
				out = append(out, h)
			}
		}

		err = rows.Err()
		rows.Close()

		return out, err
	})
	if len(missing) > 0 {
		return nil, errors.New("warning lookup incomplete")
	}

	return events, err
}

func (c *Cluster) PlacementLease() (*placement.Lease, error) {
	repo, ok := c.Placement.(*placement.ConfigMap)
	if !ok {
		return nil, errors.New("placement lease requires Kubernetes repository")
	}

	return &placement.Lease{Client: repo.Client, Origin: repo.Origin, Namespace: repo.Namespace, Name: repo.Name + "-recovery", Token: repo.Token, Holder: correlationID(), Duration: 15 * time.Second}, nil
}

func offlineDataNode(name, url string) (*DataNode, error) {
	ingestConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	ingestConfig.MaxConns = 4
	ingestConfig.ConnConfig.RuntimeParams["timezone"] = "UTC"
	ingestConfig.ConnConfig.ConnectTimeout = 2 * time.Second

	queryConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	queryConfig.MaxConns = 8
	queryConfig.ConnConfig.RuntimeParams["timezone"] = "UTC"
	queryConfig.ConnConfig.ConnectTimeout = 2 * time.Second

	ingest, err := pgxpool.NewWithConfig(context.Background(), ingestConfig)
	if err != nil {
		return nil, err
	}

	query, err := pgxpool.NewWithConfig(context.Background(), queryConfig)
	if err != nil {
		ingest.Close()
		return nil, err
	}

	coordinator, err := pgxpool.NewWithConfig(context.Background(), ingestConfig.Copy())
	if err != nil {
		ingest.Close()
		query.Close()

		return nil, err
	}

	store := &Store{ingest: ingest, query: query, coordination: coordinator}

	node := &DataNode{Name: name, Store: store, Shards: map[string]*Store{}}
	for i := 0; i < placement.Shards; i++ {
		node.Shards[fmt.Sprintf("shard-%d", i)] = &Store{ingest: ingest, query: query, namespace: fmt.Sprintf("shard_%d", i)}
	}

	return node, nil
}
