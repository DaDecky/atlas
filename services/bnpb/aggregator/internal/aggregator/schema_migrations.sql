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

CREATE TABLE IF NOT EXISTS stream_state (
    source text PRIMARY KEY REFERENCES source_progress(source),
    committed_offset bigint NOT NULL CHECK (committed_offset >= 0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);
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
CREATE TABLE IF NOT EXISTS unprocessed_payload_chunks (
 message_id text NOT NULL REFERENCES unprocessed_messages(message_id) ON DELETE CASCADE,
 part bigint NOT NULL CHECK (part >= 0),
 data bytea NOT NULL CHECK (octet_length(data) BETWEEN 1 AND 262144),
 PRIMARY KEY(message_id,part)
);
CREATE TABLE IF NOT EXISTS field_reports_inbox (
 report_id text PRIMARY KEY, payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 received_at timestamptz NOT NULL
);
COMMIT;
