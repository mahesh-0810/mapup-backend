-- Geofencing & vehicle tracking schema
-- Applied automatically on first container start via docker-entrypoint-initdb.d,
-- or manually with: psql -U postgres -d geofence_db -f schema.sql

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS vehicles (
    id             VARCHAR(64) PRIMARY KEY,
    vehicle_number VARCHAR(32) NOT NULL,
    driver_name    VARCHAR(128),
    vehicle_type   VARCHAR(64),
    phone          VARCHAR(32),
    status         VARCHAR(16) NOT NULL DEFAULT 'active',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS geofences (
    id          VARCHAR(64) PRIMARY KEY,
    name        VARCHAR(128) NOT NULL,
    description TEXT,
    category    VARCHAR(64),
    boundaries  GEOMETRY(POLYGON, 4326) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_geofences_boundaries ON geofences USING GIST (boundaries);

CREATE TABLE IF NOT EXISTS vehicle_locations (
    id          SERIAL PRIMARY KEY,
    vehicle_id  VARCHAR(64) NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
    location    GEOMETRY(POINT, 4326) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_vehicle_time ON vehicle_locations (vehicle_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_location ON vehicle_locations USING GIST (location);

CREATE TABLE IF NOT EXISTS alert_rules (
    id          VARCHAR(64) PRIMARY KEY,
    geofence_id VARCHAR(64) NOT NULL REFERENCES geofences(id) ON DELETE CASCADE,
    vehicle_id  VARCHAR(64) NOT NULL DEFAULT '', -- '' means "applies to all vehicles"
    event_type  VARCHAR(16) NOT NULL, -- entry | exit | both
    status      VARCHAR(16) NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alert_rules_geofence ON alert_rules (geofence_id);
CREATE INDEX IF NOT EXISTS idx_alert_rules_vehicle ON alert_rules (vehicle_id);

CREATE TABLE IF NOT EXISTS violations (
    id          VARCHAR(64) PRIMARY KEY,
    vehicle_id  VARCHAR(64) NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
    geofence_id VARCHAR(64) NOT NULL REFERENCES geofences(id) ON DELETE CASCADE,
    event_type  VARCHAR(16) NOT NULL, -- entry | exit
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_violations_timestamp ON violations (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_violations_vehicle ON violations (vehicle_id);
CREATE INDEX IF NOT EXISTS idx_violations_geofence ON violations (geofence_id);
