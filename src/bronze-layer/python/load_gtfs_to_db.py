"""
Loads GTFS zip files from the bronze layer into the PostgreSQL/PostGIS silver layer.

Usage:
    # Load a specific file
    python load_gtfs_to_db.py path/to/GTFS_FP2026_20260506.zip

    # Discover and load all unloaded ZIPs from the bronze data directory
    python load_gtfs_to_db.py

Feed date is extracted from the filename. Each feed is loaded atomically:
if any step fails the whole feed is rolled back and can be retried cleanly.
"""

import csv
import io
import json
import logging
import os
import re
import sys
import zipfile
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_HERE = Path(__file__).parent          # src/silver-layer/python/
_REPO_ROOT = _HERE.parent.parent.parent  # repo root
load_dotenv(_REPO_ROOT / "infra" / ".env")

BRONZE_DATA_DIR = _REPO_ROOT / "data" / "gtfs-static"
LOG_FILE = _REPO_ROOT / "load_log.txt"
BATCH_SIZE = 20000 # rows per execute_values call

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FILE.touch(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type converters
# ---------------------------------------------------------------------------
def _str(v):
    return v.strip() if v and v.strip() else None

def _int(v):
    return int(v.strip()) if v and v.strip() else None

def _float(v):
    return float(v.strip()) if v and v.strip() else None

def _bool(v):
    return bool(int(v.strip())) if v and v.strip() else None

def _gtfs_date(v):
    v = (v or "").strip()
    if len(v) == 8:
        return date(int(v[:4]), int(v[4:6]), int(v[6:8]))
    return None

# ---------------------------------------------------------------------------
# Table specs
# Each entry: (gtfs_column_name, type_converter, value_if_column_absent)
# ---------------------------------------------------------------------------
AGENCY_SPEC = [
    ("agency_id",        _str,   ""),
    ("agency_name",      _str,   None),
    ("agency_url",       _str,   None),
    ("agency_timezone",  _str,   None),
    ("agency_lang",      _str,   None),
    ("agency_phone",     _str,   None),
    ("agency_fare_url",  _str,   None),
    ("agency_email",     _str,   None),
]

STOPS_SPEC = [
    ("stop_id",              _str,   None),
    ("stop_code",            _str,   None),
    ("stop_name",            _str,   None),
    ("stop_desc",            _str,   None),
    ("stop_lat",             _float, None),
    ("stop_lon",             _float, None),
    ("zone_id",              _str,   None),
    ("stop_url",             _str,   None),
    ("location_type",        _int,   None),
    ("parent_station",       _str,   None),
    ("stop_timezone",        _str,   None),
    ("wheelchair_boarding",  _int,   None),
]

ROUTES_SPEC = [
    ("route_id",          _str,   None),
    ("agency_id",         _str,   None),
    ("route_short_name",  _str,   None),
    ("route_long_name",   _str,   None),
    ("route_desc",        _str,   None),
    ("route_type",        _int,   None),
    ("route_url",         _str,   None),
    ("route_color",       _str,   None),
    ("route_text_color",  _str,   None),
    ("route_sort_order",  _int,   None),
]

TRIPS_SPEC = [
    ("trip_id",               _str,   None),
    ("route_id",              _str,   None),
    ("service_id",            _str,   None),
    ("trip_headsign",         _str,   None),
    ("trip_short_name",       _str,   None),
    ("direction_id",          _int,   None),
    ("block_id",              _str,   None),
    ("shape_id",              _str,   None),
    ("wheelchair_accessible", _int,   None),
    ("bikes_allowed",         _int,   None),
]

STOP_TIMES_SPEC = [
    ("trip_id",             _str,   None),
    ("arrival_time",        _str,   None),
    ("departure_time",      _str,   None),
    ("stop_id",             _str,   None),
    ("stop_sequence",       _int,   None),
    ("stop_headsign",       _str,   None),
    ("pickup_type",         _int,   None),
    ("drop_off_type",       _int,   None),
    ("shape_dist_traveled", _float, None),
    ("timepoint",           _int,   None),
]

CALENDAR_SPEC = [
    ("service_id",  _str,       None),
    ("monday",      _bool,      None),
    ("tuesday",     _bool,      None),
    ("wednesday",   _bool,      None),
    ("thursday",    _bool,      None),
    ("friday",      _bool,      None),
    ("saturday",    _bool,      None),
    ("sunday",      _bool,      None),
    ("start_date",  _gtfs_date, None),
    ("end_date",    _gtfs_date, None),
]

CALENDAR_DATES_SPEC = [
    ("service_id",     _str,       None),
    ("date",           _gtfs_date, None),
    ("exception_type", _int,       None),
]

SHAPES_SPEC = [
    ("shape_id",             _str,   None),
    ("shape_pt_lat",         _float, None),
    ("shape_pt_lon",         _float, None),
    ("shape_pt_sequence",    _int,   None),
    ("shape_dist_traveled",  _float, None),
]

TRANSFERS_SPEC = [
    ("from_stop_id",      _str,   None),
    ("to_stop_id",        _str,   None),
    ("transfer_type",     _int,   None),
    ("min_transfer_time", _int,   None),
]

FEED_INFO_SPEC = [
    ("feed_publisher_name", _str,       None),
    ("feed_publisher_url",  _str,       None),
    ("feed_lang",           _str,       None),
    ("feed_start_date",     _gtfs_date, None),
    ("feed_end_date",       _gtfs_date, None),
    ("feed_version",        _str,       None),
]

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        # host=os.getenv("POSTGRES_HOST", "localhost"),
        # port=int(os.getenv("POSTGRES_PORT", 5432)),
        # dbname=os.getenv("POSTGRES_DB", "swiss_trafic"),
        # user=os.getenv("POSTGRES_USER", "gtfs"),
        # password=os.getenv("POSTGRES_PASSWORD", "")
        sslmode="require",
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )


def is_already_loaded(conn, feed_date: date) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM staging_gtfs_static.log_feed_loads WHERE feed_date = %s", (feed_date,)
        )
        return cur.fetchone() is not None

# ---------------------------------------------------------------------------
# ZIP / CSV helpers
# ---------------------------------------------------------------------------
def extract_feed_date(filename: str) -> date:
    """Parse yyyymmdd or yyyy-mm-dd from the zip filename."""
    m = re.search(r"_(\d{8})\.zip", filename, re.IGNORECASE)
    if m:
        d = m.group(1)
        return date(int(d[:4]), int(d[4:6]), int(d[6:8]))
    m = re.search(r"_(\d{4}-\d{2}-\d{2})\.zip", filename, re.IGNORECASE)
    if m:
        return date.fromisoformat(m.group(1))
    raise ValueError(f"Cannot extract date from filename: {filename}")


def find_in_zip(zf: zipfile.ZipFile, filename: str) -> str | None:
    """Return the full zip path for a GTFS file, ignoring sub-folders."""
    for name in zf.namelist():
        if Path(name).name.lower() == filename.lower():
            return name
    return None


def stream_rows(zf: zipfile.ZipFile, filename: str):
    """Yield csv.DictReader rows from a GTFS file inside the zip."""
    entry = find_in_zip(zf, filename)
    if entry is None:
        return
    with zf.open(entry) as raw:
        wrapper = io.TextIOWrapper(raw, encoding="utf-8-sig")
        reader = csv.DictReader(wrapper)
        for row in reader:
            yield row

# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------
def _make_tuple(row: dict, spec: list, feed_date: date) -> tuple:
    values = [feed_date]
    for col, converter, default in spec:
        if col in row:
            try:
                values.append(converter(row[col]))
            except (ValueError, TypeError):
                values.append(default)
        else:
            values.append(default)
    return tuple(values)


def _flush_batch(conn, table: str, spec: list, batch: list) -> None:
    cols = ["feed_date"] + [col for col, _, _ in spec]
    psycopg2.extras.execute_values(
        conn.cursor(),
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING",
        batch,
        page_size=BATCH_SIZE,
    )


def load_table(
    conn,
    zf: zipfile.ZipFile,
    gtfs_file: str,
    table: str,
    spec: list,
    feed_date: date,
    required: bool = True,
) -> int:
    entry = find_in_zip(zf, gtfs_file)
    if entry is None:
        if required:
            raise FileNotFoundError(f"{gtfs_file} not found in zip")
        log.info("  %s not present, skipping.", gtfs_file)
        return 0

    log.info("  Loading %s → %s", gtfs_file, table)
    count = 0
    batch: list = []

    for row in stream_rows(zf, gtfs_file):
        batch.append(_make_tuple(row, spec, feed_date))
        if len(batch) >= BATCH_SIZE:
            _flush_batch(conn, table, spec, batch)
            count += len(batch)
            batch = []
            log.info("    ... %d rows inserted so far", count)

    if batch:
        _flush_batch(conn, table, spec, batch)
        count += len(batch)

    log.info("  → %d rows loaded into %s", count, table)
    return count

# ---------------------------------------------------------------------------
# Post-load geometry builders
# ---------------------------------------------------------------------------
def build_stop_geometries(conn, feed_date: date) -> None:
    log.info("  Building stop point geometries...")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE staging_gtfs_static.stops
               SET geom = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)
             WHERE feed_date = %s
               AND stop_lat IS NOT NULL
               AND stop_lon IS NOT NULL
            """,
            (feed_date,),
        )


def build_shape_geometries(conn, feed_date: date) -> None:
    log.info("  Building shape linestring geometries...")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO staging_gtfs_static.shape_geoms (feed_date, shape_id, geom)
            SELECT
                feed_date,
                shape_id,
                ST_MakeLine(
                    ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326)
                    ORDER BY shape_pt_sequence
                )
            FROM staging_gtfs_static.shapes
            WHERE feed_date = %s
              AND shape_pt_lat IS NOT NULL
              AND shape_pt_lon IS NOT NULL
            GROUP BY feed_date, shape_id
            ON CONFLICT DO NOTHING
            """,
            (feed_date,),
        )

# ---------------------------------------------------------------------------
# Main load routine
# ---------------------------------------------------------------------------
def load_zip(zip_path: Path) -> None:
    feed_date = extract_feed_date(zip_path.name)
    log.info("=" * 60)
    log.info("Processing %s  (feed_date=%s)", zip_path.name, feed_date)

    conn = get_conn()
    try:
        if is_already_loaded(conn, feed_date):
            log.info("Already loaded — skipping.")
            return

        with zipfile.ZipFile(zip_path) as zf:
            rows: dict[str, int] = {}

            # Required files
            rows["agency"]         = load_table(conn, zf, "agency.txt",         "staging_gtfs_static.agency",         AGENCY_SPEC,         feed_date, required=True)
            rows["stops"]          = load_table(conn, zf, "stops.txt",          "staging_gtfs_static.stops",          STOPS_SPEC,          feed_date, required=True)
            rows["routes"]         = load_table(conn, zf, "routes.txt",         "staging_gtfs_static.routes",         ROUTES_SPEC,         feed_date, required=True)
            rows["trips"]          = load_table(conn, zf, "trips.txt",          "staging_gtfs_static.trips",          TRIPS_SPEC,          feed_date, required=True)
            rows["stop_times"]     = load_table(conn, zf, "stop_times.txt",     "staging_gtfs_static.stop_times",     STOP_TIMES_SPEC,     feed_date, required=True)

            # Optional files
            rows["calendar"]       = load_table(conn, zf, "calendar.txt",       "staging_gtfs_static.calendar",       CALENDAR_SPEC,       feed_date, required=False)
            rows["calendar_dates"] = load_table(conn, zf, "calendar_dates.txt", "staging_gtfs_static.calendar_dates", CALENDAR_DATES_SPEC, feed_date, required=False)
            rows["shapes"]         = load_table(conn, zf, "shapes.txt",         "staging_gtfs_static.shapes",         SHAPES_SPEC,         feed_date, required=False)
            rows["transfers"]      = load_table(conn, zf, "transfers.txt",      "staging_gtfs_static.transfers",      TRANSFERS_SPEC,      feed_date, required=False)
            rows["feed_info"]      = load_table(conn, zf, "feed_info.txt",      "staging_gtfs_static.feed_info",      FEED_INFO_SPEC,      feed_date, required=False)

        # This will be moved to the silver layer in the future
        # Geometry post-processing (outside the ZipFile context)
        # build_stop_geometries(conn, feed_date)
        # if rows.get("shapes", 0) > 0:
        #     build_shape_geometries(conn, feed_date)

        # Record successful load
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO staging_gtfs_static.log_feed_loads (feed_date, filename, rows_loaded) VALUES (%s, %s, %s)",
                (feed_date, zip_path.name, json.dumps(rows)),
            )

        conn.commit()
        log.info("Successfully loaded %s", zip_path.name)

    except Exception as exc: 
        try:
            conn.rollback()
        except Exception:
            pass
        log.error("FAILED to load %s: %s", zip_path.name, exc, exc_info=True)
        raise
    finally:
        conn.close()


def main() -> None:
    if len(sys.argv) > 1:
        zip_paths = [Path(sys.argv[1])]
    else:
        zip_paths = sorted(BRONZE_DATA_DIR.rglob("*.zip"))
        if not zip_paths:
            log.warning("No ZIP files found in %s", BRONZE_DATA_DIR)
            return
        log.info("Found %d ZIP file(s) in the bronze layer.", len(zip_paths))

    for zip_path in zip_paths:
        load_zip(zip_path)


if __name__ == "__main__":
    main()
