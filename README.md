# Swiss Traffic — GTFS Data Pipeline

A data pipeline for downloading and storing the Swiss public transport timetable (GTFS 2026) published by [opentransportdata.swiss](https://data.opentransportdata.swiss/en/dataset/timetable-2026-gtfs2020).

The project follows a two-layer architecture:

```
swiss_trafic/
├── bronze-layer/        # Raw downloads from the source
├── silver-layer/        # Cleaned, structured PostgreSQL/PostGIS database
├── data/                # Downloaded ZIP files (git-ignored)
├── requirements.txt     # Python dependencies
└── .gitignore
```

---

## Requirements

- Python 3.11+
- Docker + Docker Compose

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---

## Bronze Layer — Download GTFS files

**Script:** `bronze-layer/download_gtfs_static.py`

Scrapes the dataset page, extracts all available GTFS ZIP links, and downloads any that are not already present locally.

### Configuration (top of the script)

| Variable | Default | Description |
|---|---|---|
| `MAX_FILES` | `5` | Number of most-recent files to download. Set to `None` for all (~50+ files, ~7 GB). |
| `DATA_DIR` | `../data/gtfs-static` | Destination folder (relative to the script). |
| `LOG_FILE` | `../log_download_static.txt` | Download log. |

### Folder structure

Files are organised by year-month to make navigation easier:

```
data/
└── gtfs-static/
    ├── 202605/
    │   ├── GTFS_FP2026_20260506.zip
    │   └── GTFS_FP2026_20260503.zip
    ├── 202604/
    │   └── ...
    └── ...
```

### Usage

```bash
python bronze-layer/download_gtfs_static.py
```

- Already-downloaded files are skipped automatically.
- Partial files from failed downloads are deleted so re-runs retry cleanly.
- All activity is logged to `log_download_static.txt`.

---

## Silver Layer — PostgreSQL + PostGIS database

**Directory:** `silver-layer/`

Loads the raw GTFS ZIP files into a structured relational database with spatial extensions.

### Database schema (`init/01_schema.sql`)

All tables live in the `gtfs` schema. Every table includes a `feed_date` column so multiple timetable snapshots coexist without overwriting each other.

| Table | GTFS file | Notes |
|---|---|---|
| `gtfs.agency` | `agency.txt` | |
| `gtfs.stops` | `stops.txt` | Includes a PostGIS `geom` (Point) column |
| `gtfs.routes` | `routes.txt` | |
| `gtfs.trips` | `trips.txt` | |
| `gtfs.stop_times` | `stop_times.txt` | Largest table (~10 M rows per feed) |
| `gtfs.calendar` | `calendar.txt` | Optional |
| `gtfs.calendar_dates` | `calendar_dates.txt` | Optional |
| `gtfs.shapes` | `shapes.txt` | Optional |
| `gtfs.shape_geoms` | — | Aggregated PostGIS LineString per shape, built after load |
| `gtfs.transfers` | `transfers.txt` | Optional |
| `gtfs.feed_info` | `feed_info.txt` | Optional |
| `gtfs.feed_loads` | — | Tracks which feeds have been loaded |

### Setup

**1. Configure credentials**

```bash
cp silver-layer/.env.example silver-layer/.env
# Edit silver-layer/.env and set POSTGRES_PASSWORD
```

**2. Start the database**

The PostGIS image automatically runs `init/01_schema.sql` on first start.

```bash
cd silver-layer
docker compose up -d
```

**3. Load GTFS data**

```bash
# Load all downloaded ZIPs
python silver-layer/load_gtfs_to_db.py

# Or load a single file
python silver-layer/load_gtfs_to_db.py data/gtfs-static/202605/GTFS_FP2026_20260506.zip
```

- Each ZIP is loaded as a single atomic transaction (all-or-nothing).
- Already-loaded feeds are skipped based on the `gtfs.feed_loads` table.
- Point and linestring geometries are built with PostGIS after each feed is inserted.
- Load activity is logged to `silver-layer/load_log.txt`.

---

## Data Source

| Field | Value |
|---|---|
| Publisher | Business office SKI / opentransportdata.swiss |
| Dataset | [Timetable 2026 (GTFS2020)](https://data.opentransportdata.swiss/en/dataset/timetable-2026-gtfs2020) |
| Format | GTFS Static (ZIP) |
| Coverage | December 14, 2025 – December 12, 2026 |
| Update interval | Twice a week |
| License | Non-commercial allowed / Commercial allowed / Reference required |
