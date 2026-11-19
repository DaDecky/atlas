#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres --set=owner_password="$CANONICAL_DB_PASSWORD" <<'SQL'
CREATE ROLE aggregator LOGIN PASSWORD :'owner_password';
CREATE DATABASE canonical OWNER aggregator;
REVOKE CONNECT ON DATABASE canonical FROM PUBLIC;
GRANT CONNECT ON DATABASE canonical TO aggregator;
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname canonical <<'SQL'
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO aggregator;
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname canonical <<'SQL'
DO $$
DECLARE i integer; owner_name text; schema_name text;
BEGIN
 FOR i IN 0..47 LOOP
  owner_name := 'canonical_shard_' || i;
  schema_name := 'shard_' || i;
  EXECUTE format('CREATE ROLE %I NOLOGIN', owner_name);
  EXECUTE format('GRANT %I TO aggregator', owner_name);
  EXECUTE format('CREATE SCHEMA %I AUTHORIZATION %I', schema_name, owner_name);
  EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', schema_name);
 END LOOP;
END $$;
SQL

# Persist one identity for this data directory. A recreated volume gets a new ID.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname canonical <<'SQL'
SET ROLE aggregator;
CREATE TABLE public.store_identity (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 id text NOT NULL DEFAULT gen_random_uuid()::text
);
INSERT INTO public.store_identity(singleton) VALUES(true);
SQL
