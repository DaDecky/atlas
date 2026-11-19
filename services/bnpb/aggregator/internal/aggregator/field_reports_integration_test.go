package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"atlas/services/bnpb/aggregator/internal/placement"
)

func testFieldReport(t *testing.T) FieldReport {
	t.Helper()

	report, err := DecodeFieldReport([]byte(`{"report_id":"report-concurrent","observed_at":"2026-11-18T00:00:00Z","location":{"latitude":-6.2,"longitude":106.8,"area_name":"Jakarta"},"category":"COASTAL","message":"Rising water","client_id":"field-team"}`))
	if err != nil {
		t.Fatal(err)
	}

	return report
}

func TestIntegrationFieldReportConcurrentEffectAndRedeliveryBeforeAck(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	report := testFieldReport(t)

	var (
		created atomic.Int32
		wg      sync.WaitGroup
	)
	for range 20 {
		wg.Go(func() {
			_, inserted, err := s.ApplyFieldReport(ctx, report)
			if err != nil {
				t.Error(err)
			}

			if inserted {
				created.Add(1)
			}
		})
	}

	wg.Wait()

	if created.Load() != 1 {
		t.Fatal("duplicate business effect", created.Load())
	}

	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Fatal("run make integration with RabbitMQ")
	}

	connect := func() (*amqp.Connection, *amqp.Channel) {
		t.Helper()

		conn, err := amqp.Dial(url)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() { _ = conn.Close() })

		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}

		if err = DeclareFieldReports(ch); err != nil {
			t.Fatal(err)
		}

		return conn, ch
	}

	conn, ch := connect()
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}

	raw, _ := json.Marshal(report)

	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", FieldReportsQueue, true, false, amqp.Publishing{MessageId: report.ReportID, Type: "FieldReport", DeliveryMode: amqp.Persistent, Body: raw})
	if err != nil || confirmation == nil || !confirmation.Wait() {
		t.Fatal("publication unconfirmed", err)
	}

	delivery, ok, err := ch.Get(FieldReportsQueue, false)
	if err != nil || !ok {
		t.Fatal("queue delivery", ok, err)
	}

	consumer := &FieldReportsConsumer{Store: s, Log: testLog()}
	if err = consumer.Process(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	// Drop the receiver after commit but before ack. The real broker requeues it.
	_ = conn.Close()

	reopened, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4, defaultDBConnectTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	consumer.Store = reopened
	_, ch = connect()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		delivery, ok, err = ch.Get(FieldReportsQueue, false)
		if err != nil || ok {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if err != nil || !ok || !delivery.Redelivered {
		t.Fatal("unacked report not redelivered", ok, err)
	}

	if err = consumer.Process(ctx, delivery); err != nil {
		t.Fatal(err)
	}

	if err = delivery.Ack(false); err != nil {
		t.Fatal(err)
	}

	var count int
	if err = reopened.query.QueryRow(ctx, `SELECT count(*) FROM field_reports_inbox WHERE report_id=$1`, report.ReportID).Scan(&count); err != nil || count != 1 {
		t.Fatal("redelivery effect", count, err)
	}

	t.Log("20 concurrent deliveries and crash after commit before ack retained one report effect")
}

func TestDataIntegrationFieldReportsDualCopyRefill(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	report := testFieldReport(t)

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if _, _, err := c.ApplyFieldReport(ctx, report); err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	shard := placement.Shard("field-report:" + report.ReportID)
	copies := table.Shards[shard]

	from, to := c.Nodes[copies[0].Node].Shards[shard], c.Nodes[copies[1].Node].Shards[shard]
	for _, store := range []*Store{from, to} {
		tx, err := store.begin(ctx, true)
		if err != nil {
			t.Fatal(err)
		}

		var count int

		err = tx.QueryRow(ctx, `SELECT count(*) FROM field_reports_inbox WHERE report_id=$1`, report.ReportID).Scan(&count)
		_ = tx.Rollback(ctx)

		if err != nil || count != 1 {
			t.Fatal("missing report replica", count, err)
		}
	}

	tx, err := to.begin(ctx, false)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = tx.Exec(ctx, `TRUNCATE field_reports_inbox`); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	before, err := shardDigest(ctx, to)
	if err != nil {
		t.Fatal(err)
	}

	want, err := shardDigest(ctx, from)
	if err != nil || before == want {
		t.Fatal("digest did not notice missing report", err)
	}

	if err = copyShard(ctx, from, to); err != nil {
		t.Fatal(err)
	}

	after, err := shardDigest(ctx, to)
	if err != nil || after != want {
		t.Fatal("refill lost report effect", err)
	}

	t.Log("report applied once on each holder; missing inbox detected and refilled")
}

func TestDataIntegrationFieldReportsDuringHolderReplacement(t *testing.T) {
	c := dataCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	node, err := OpenDataNode(ctx, "d", os.Getenv("DATA_TEST_D"), DefaultClusterLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer node.Store.Close()

	c.Nodes["d"] = node
	report := testFieldReport(t)
	shard := placement.Shard("field-report:" + report.ReportID)

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ids := []string{}
	for i := 0; len(ids) < 60; i++ {
		id := fmt.Sprintf("moving-report-%d", i)
		if placement.Shard("field-report:"+id) == shard {
			ids = append(ids, id)
		}
	}

	started := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		close(started)

		for _, id := range ids {
			current := report

			current.ReportID = id
			for {
				_, _, err := c.ApplyFieldReport(ctx, current)
				if err == nil {
					break
				}

				if ctx.Err() != nil {
					done <- err
					return
				}

				time.Sleep(5 * time.Millisecond)
			}
		}

		done <- nil
	}()

	<-started

	if err = c.MoveCopy(ctx, shard, table.Shards[shard][0].Node, "d"); err != nil {
		t.Fatal(err)
	}

	if err = <-done; err != nil {
		t.Fatal(err)
	}

	table, _, err = c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, copy := range table.Shards[shard] {
		for _, id := range ids {
			_, found, err := c.Nodes[copy.Node].Shards[shard].fieldReport(ctx, id)
			if err != nil || !found {
				t.Fatal("acknowledged report missing after holder replacement", copy.Node, id, err)
			}
		}
	}

	t.Log("all 60 reports acknowledged during replacement remained on both final holders")
}
