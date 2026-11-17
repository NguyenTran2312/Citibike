-- Runs on first initialization of the postgres-data volume.
-- Consumers convert GBFS epoch seconds into TIMESTAMPTZ before writing.
CREATE TABLE IF NOT EXISTS station_status_latest (
    station_id TEXT PRIMARY KEY,
    poll_id TEXT NOT NULL,
    num_bikes_available INTEGER NOT NULL CHECK (num_bikes_available >= 0),
    num_docks_available INTEGER CHECK (num_docks_available >= 0),
    is_installed BOOLEAN NOT NULL,
    is_renting BOOLEAN NOT NULL,
    is_returning BOOLEAN NOT NULL,
    last_updated TIMESTAMPTZ,
    last_reported TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS active_alerts (
    station_id TEXT NOT NULL REFERENCES station_status_latest(station_id),
    alert_type TEXT NOT NULL CHECK (alert_type IN ('LOW_BIKES', 'EMPTY', 'UNAVAILABLE', 'STALE')),
    opened_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (station_id, alert_type)
);
COMMENT ON TABLE station_status_latest IS
    'Consumer UPSERT must compare observation timestamps to reject older messages.';
COMMENT ON TABLE active_alerts IS
    'Consumer/reconciliation maintains alerts; STALE must also advance when no messages arrive.';
