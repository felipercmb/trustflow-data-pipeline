PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS settings (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    dataset_kind TEXT NOT NULL CHECK (dataset_kind IN ('synthetic', 'operational')),
    sla_seconds INTEGER NOT NULL CHECK (sla_seconds > 0)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    raw_sha256 TEXT NOT NULL,
    raw_key TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running', 'accepted', 'rejected')),
    input_rows INTEGER NOT NULL DEFAULT 0,
    inserted_rows INTEGER NOT NULL DEFAULT 0,
    duplicate_rows INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('requested','signed','failed','canceled','expired')),
    occurred_at TEXT NOT NULL,
    occurred_at_us INTEGER NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('web','mobile','api')),
    document_type TEXT NOT NULL CHECK (document_type IN ('contract','authorization','service_order')),
    failure_code TEXT CHECK (failure_code IN ('PROVIDER_UNAVAILABLE','INVALID_DOCUMENT','TIMEOUT')),
    payload_sha256 TEXT NOT NULL,
    source_batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    CHECK ((event_type = 'failed' AND failure_code IS NOT NULL)
        OR (event_type <> 'failed' AND failure_code IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_batch ON events(source_batch_id);
CREATE INDEX IF NOT EXISTS idx_batches_raw_status ON batches(raw_sha256, status);

CREATE VIEW IF NOT EXISTS workflow_lifecycle AS
SELECT workflow_id, MIN(channel) AS channel, MIN(document_type) AS document_type,
    MAX(CASE WHEN event_type = 'requested' THEN occurred_at END) AS requested_at,
    MAX(CASE WHEN event_type <> 'requested' THEN occurred_at END) AS terminal_at,
    MAX(CASE WHEN event_type <> 'requested' THEN event_type END) AS terminal_type,
    MAX(failure_code) AS failure_code,
    CASE
        WHEN SUM(event_type = 'requested') = 0 THEN 'awaiting_request'
        WHEN SUM(event_type <> 'requested') = 0 THEN 'pending'
        ELSE MAX(CASE WHEN event_type <> 'requested' THEN event_type END)
    END AS status
FROM events
GROUP BY workflow_id;
