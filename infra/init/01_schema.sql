-- PostGIS is pre-installed by the postgis/postgis image; ensure it exists.
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS staging_gtfs_static;

-- ---------------------------------------------------------------------------
-- Feed load tracking
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.log_feed_loads (
    feed_date    DATE        PRIMARY KEY,
    filename     TEXT        NOT NULL,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_loaded  JSONB
);

-- ---------------------------------------------------------------------------
-- agency.txt
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.agency (
    feed_date        DATE NOT NULL,
    agency_id        TEXT NOT NULL DEFAULT '',
    agency_name      TEXT,
    agency_url       TEXT,
    agency_timezone  TEXT,
    agency_lang      TEXT,
    agency_phone     TEXT,
    agency_fare_url  TEXT,
    agency_email     TEXT,
    PRIMARY KEY (feed_date, agency_id)
);

-- ---------------------------------------------------------------------------
-- stops.txt
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.stops (
    feed_date             DATE NOT NULL,
    stop_id               TEXT NOT NULL,
    stop_code             TEXT,
    stop_name             TEXT,
    stop_desc             TEXT,
    stop_lat              DOUBLE PRECISION,
    stop_lon              DOUBLE PRECISION,
    zone_id               TEXT,
    stop_url              TEXT,
    location_type         SMALLINT,
    parent_station        TEXT,
    stop_timezone         TEXT,
    wheelchair_boarding   SMALLINT,
    -- geom                  GEOMETRY(Point, 4326),   -- populated after insert
    PRIMARY KEY (feed_date, stop_id)
);
CREATE INDEX IF NOT EXISTS stops_geom_idx ON staging_gtfs_static.stops USING GIST (geom);

-- ---------------------------------------------------------------------------
-- routes.txt
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.routes (
    feed_date          DATE NOT NULL,
    route_id           TEXT NOT NULL,
    agency_id          TEXT,
    route_short_name   TEXT,
    route_long_name    TEXT,
    route_desc         TEXT,
    route_type         SMALLINT,
    route_url          TEXT,
    route_color        TEXT,
    route_text_color   TEXT,
    route_sort_order   INTEGER,
    PRIMARY KEY (feed_date, route_id)
);

-- ---------------------------------------------------------------------------
-- trips.txt
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.trips (
    feed_date               DATE NOT NULL,
    trip_id                 TEXT NOT NULL,
    route_id                TEXT NOT NULL,
    service_id              TEXT NOT NULL,
    trip_headsign           TEXT,
    trip_short_name         TEXT,
    direction_id            SMALLINT,
    block_id                TEXT,
    shape_id                TEXT,
    wheelchair_accessible   SMALLINT,
    bikes_allowed           SMALLINT,
    PRIMARY KEY (feed_date, trip_id)
);
CREATE INDEX IF NOT EXISTS trips_route_idx   ON staging_gtfs_static.trips (feed_date, route_id);
CREATE INDEX IF NOT EXISTS trips_service_idx ON staging_gtfs_static.trips (feed_date, service_id);

-- ---------------------------------------------------------------------------
-- stop_times.txt  (largest table — ~10 M rows per feed)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.stop_times (
    feed_date            DATE NOT NULL,
    trip_id              TEXT NOT NULL,
    arrival_time         TEXT,           -- stored as text: values like "25:30:00" are valid
    departure_time       TEXT,
    stop_id              TEXT NOT NULL,
    stop_sequence        INTEGER NOT NULL,
    stop_headsign        TEXT,
    pickup_type          SMALLINT,
    drop_off_type        SMALLINT,
    shape_dist_traveled  DOUBLE PRECISION,
    timepoint            SMALLINT,
    PRIMARY KEY (feed_date, trip_id, stop_sequence)
);
CREATE INDEX IF NOT EXISTS stop_times_stop_idx ON staging_gtfs_static.stop_times (feed_date, stop_id);

-- ---------------------------------------------------------------------------
-- calendar.txt  (optional — some feeds use calendar_dates only)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.calendar (
    feed_date   DATE NOT NULL,
    service_id  TEXT NOT NULL,
    monday      BOOLEAN NOT NULL,
    tuesday     BOOLEAN NOT NULL,
    wednesday   BOOLEAN NOT NULL,
    thursday    BOOLEAN NOT NULL,
    friday      BOOLEAN NOT NULL,
    saturday    BOOLEAN NOT NULL,
    sunday      BOOLEAN NOT NULL,
    start_date  DATE NOT NULL,
    end_date    DATE NOT NULL,
    PRIMARY KEY (feed_date, service_id)
);

-- ---------------------------------------------------------------------------
-- calendar_dates.txt  (optional)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.calendar_dates (
    feed_date       DATE NOT NULL,
    service_id      TEXT NOT NULL,
    date            DATE NOT NULL,
    exception_type  SMALLINT NOT NULL,
    PRIMARY KEY (feed_date, service_id, date)
);

-- ---------------------------------------------------------------------------
-- shapes.txt  (optional — individual points)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.shapes (
    feed_date             DATE NOT NULL,
    shape_id              TEXT NOT NULL,
    shape_pt_lat          DOUBLE PRECISION NOT NULL,
    shape_pt_lon          DOUBLE PRECISION NOT NULL,
    shape_pt_sequence     INTEGER NOT NULL,
    shape_dist_traveled   DOUBLE PRECISION,
    PRIMARY KEY (feed_date, shape_id, shape_pt_sequence)
);

-- Aggregated linestring per shape_id (populated after shapes are loaded)
CREATE TABLE IF NOT EXISTS staging_gtfs_static.shape_geoms (
    feed_date  DATE NOT NULL,
    shape_id   TEXT NOT NULL,
    -- geom       GEOMETRY(LineString, 4326) NOT NULL,
    PRIMARY KEY (feed_date, shape_id)
);
CREATE INDEX IF NOT EXISTS shape_geoms_geom_idx ON staging_gtfs_static.shape_geoms USING GIST (geom);

-- ---------------------------------------------------------------------------
-- transfers.txt  (optional)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.transfers (
    feed_date          DATE NOT NULL,
    from_stop_id       TEXT NOT NULL,
    to_stop_id         TEXT NOT NULL,
    transfer_type      SMALLINT NOT NULL,
    min_transfer_time  INTEGER,
    PRIMARY KEY (feed_date, from_stop_id, to_stop_id)
);

-- ---------------------------------------------------------------------------
-- feed_info.txt  (optional)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_gtfs_static.feed_info (
    feed_date             DATE NOT NULL,
    feed_publisher_name   TEXT,
    feed_publisher_url    TEXT,
    feed_lang             TEXT,
    feed_start_date       DATE,
    feed_end_date         DATE,
    feed_version          TEXT,
    PRIMARY KEY (feed_date)
);
