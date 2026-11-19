-- Objects that exist once per database, in the public schema. The shard schemas read none of them.
-- Run after schema_migrations.sql, since the fence copies the sources from source_progress.
BEGIN;
SELECT pg_advisory_xact_lock(419031, 2);

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

CREATE TABLE IF NOT EXISTS publication_outbox (
 hazard_id text NOT NULL, revision bigint NOT NULL, payload jsonb NOT NULL,
 correlation_id text NOT NULL, critical boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(hazard_id,revision)
);
CREATE INDEX IF NOT EXISTS outbox_priority ON publication_outbox(critical DESC,created_at,hazard_id,revision);
CREATE TABLE IF NOT EXISTS ingestion_clock(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),last_value timestamptz NOT NULL);
INSERT INTO ingestion_clock(singleton,last_value) VALUES(true,'epoch') ON CONFLICT DO NOTHING;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=2) THEN
 ALTER TABLE publication_outbox ADD COLUMN attempts integer NOT NULL DEFAULT 0;
 INSERT INTO schema_migrations VALUES(2);
END IF;
END $$;
COMMIT;
