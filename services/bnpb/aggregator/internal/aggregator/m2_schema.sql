-- Apply to existing M1 databases as well as fresh databases. Transactional DDL
-- and the advisory lock serialize concurrent Aggregator startup migrations.
BEGIN;
SELECT pg_advisory_xact_lock(419031, 2);

CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY);
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=1) THEN
ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_source_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_source_check
    CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI'));
ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_hazard_type_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_hazard_type_check
    CHECK (hazard_type IN ('SEISMIC', 'VOLCANIC', 'WILDFIRE', 'COASTAL'));
ALTER TABLE source_progress DROP CONSTRAINT IF EXISTS source_progress_source_check;
ALTER TABLE source_progress ADD CONSTRAINT source_progress_source_check
    CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI'));

INSERT INTO source_progress (source) VALUES ('SATELIT'), ('SENSOR_PANTAI')
    ON CONFLICT DO NOTHING;

ALTER TABLE hazard_events ALTER COLUMN source_ref_id TYPE text COLLATE "C";
CREATE INDEX IF NOT EXISTS hazard_ingested ON hazard_events (ingested_at, hazard_id);
INSERT INTO schema_migrations VALUES (1);
END IF;
END $$;

-- This identity belongs to the database, not to a shard or an Aggregator pod.
CREATE TABLE IF NOT EXISTS public.store_identity (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 id text NOT NULL DEFAULT gen_random_uuid()::text
);
INSERT INTO public.store_identity(singleton) VALUES(true) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS source_fence (
    source text PRIMARY KEY REFERENCES source_progress(source),
    epoch bigint NOT NULL CHECK (epoch > 0)
);
INSERT INTO source_fence (source, epoch)
    SELECT source, 1 FROM source_progress ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS stream_state (
    source text PRIMARY KEY REFERENCES source_progress(source),
    committed_offset bigint NOT NULL CHECK (committed_offset >= 0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);
CREATE TABLE IF NOT EXISTS publication_outbox (
 hazard_id text NOT NULL, revision bigint NOT NULL, payload jsonb NOT NULL,
 correlation_id text NOT NULL, critical boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(hazard_id,revision)
);
CREATE INDEX IF NOT EXISTS outbox_priority ON publication_outbox(critical DESC,created_at,hazard_id,revision);
CREATE TABLE IF NOT EXISTS unprocessed_messages (
 message_id text PRIMARY KEY, source text NOT NULL, received_at timestamptz NOT NULL,
 source_ref_id text NOT NULL DEFAULT '', raw bytea NOT NULL, raw_length bigint NOT NULL,
 raw_sha256 text NOT NULL, raw_cut boolean NOT NULL DEFAULT false,
 reason_code text NOT NULL, reason text NOT NULL, retryable boolean NOT NULL,
 attempts integer NOT NULL DEFAULT 0, next_attempt_at timestamptz,
 status text NOT NULL CHECK(status IN ('retrying','parked','recovered')),
 correlation_id text NOT NULL
);
CREATE INDEX IF NOT EXISTS parking_pending ON unprocessed_messages(source,received_at,message_id) WHERE status <> 'recovered';
CREATE TABLE IF NOT EXISTS ingestion_clock(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),last_value timestamptz NOT NULL);
INSERT INTO ingestion_clock(singleton,last_value) VALUES(true,'epoch') ON CONFLICT DO NOTHING;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=2) THEN
 ALTER TABLE publication_outbox ADD COLUMN attempts integer NOT NULL DEFAULT 0;
 INSERT INTO schema_migrations VALUES(2);
END IF;
END $$;
COMMIT;
