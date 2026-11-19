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

# Persist one identity for this data directory. A recreated volume gets a new ID.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname canonical <<'SQL'
SET ROLE aggregator;
CREATE TABLE public.store_identity (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 id text NOT NULL DEFAULT gen_random_uuid()::text
);
INSERT INTO public.store_identity(singleton) VALUES(true);
SQL
