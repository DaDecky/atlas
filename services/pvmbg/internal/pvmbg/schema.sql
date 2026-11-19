CREATE TABLE IF NOT EXISTS volcanic_reports (
    report_id           text             PRIMARY KEY,
    volcano_id          text             NOT NULL,
    alert_level         text             NOT NULL CHECK (alert_level IN ('Normal', 'Waspada', 'Siaga', 'Awas')),
    eruption_count_24h  integer          NOT NULL CHECK (eruption_count_24h >= 0),
    ash_column_height_m double precision NOT NULL CHECK (ash_column_height_m >= 0),
    reported_at         timestamptz      NOT NULL,
    confidence_level    double precision CHECK (confidence_level BETWEEN 0 AND 1)
);

CREATE INDEX IF NOT EXISTS volcanic_reports_time ON volcanic_reports (reported_at, report_id);

CREATE TABLE IF NOT EXISTS schema_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version   integer NOT NULL DEFAULT 1 CHECK (version IN (1, 2))
);

INSERT INTO schema_state DEFAULT VALUES ON CONFLICT DO NOTHING;

-- Repeatable M1 upgrade; existing reports keep their original wire shape.
ALTER TABLE volcanic_reports ADD COLUMN IF NOT EXISTS wire_schema_version integer
    NOT NULL DEFAULT 1 CHECK (wire_schema_version BETWEEN 1 AND 5);
ALTER TABLE schema_state DROP CONSTRAINT IF EXISTS schema_state_version_check;
ALTER TABLE schema_state ADD CONSTRAINT schema_state_version_check CHECK (version BETWEEN 1 AND 5);
