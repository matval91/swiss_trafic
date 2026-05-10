"""
Downloads GTFS timetable files from opentransportdata.swiss.

Configuration:
  MAX_FILES  - number of most-recent files to download (None = all)
  DATA_DIR   - destination folder
  LOG_FILE   - log file path
"""

import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATASET_URL = "https://data.opentransportdata.swiss/en/dataset/timetable-2026-gtfs2020"
# Internal CKAN dataset UUID (stable, embedded in every download link).
DATASET_UUID = "3d2c18f9-9ef1-463f-a249-5c67604efd74"
_HERE = Path(__file__).parent.parent
DATA_DIR = _HERE / "data" / "gtfs-static"
LOG_FILE = _HERE / "log_download_static.txt"
MAX_FILES = 5          # Set to None to download all available files.
CHUNK_SIZE = 1024 * 1024  # 1 MB read chunks during download.
REQUEST_TIMEOUT = 30   # seconds

# ---------------------------------------------------------------------------
# Logging setup — writes to both the console and download_log.txt
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
# Helpers
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "gtfs-downloader/1.0 (opentransportdata.swiss scraper)"}
    )
    return session


def fetch_resource_list(session: requests.Session) -> list[dict]:
    """
    Scrape the dataset page and return an ordered list of resources.

    Each entry is a dict with:
      - name     : display name, e.g. 'GTFS_FP2026_20260506.zip'
      - uuid     : resource UUID
      - url      : direct download URL
    """
    log.info("Fetching dataset page: %s", DATASET_URL)
    response = session.get(DATASET_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Resource links look like:
    #   /en/dataset/timetable-2026-gtfs2020/resource/<uuid>
    pattern = re.compile(
        r"/en/dataset/timetable-2026-gtfs2020/resource/"
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
        re.IGNORECASE,
    )

    seen: set[str] = set()
    resources: list[dict] = []

    for anchor in soup.find_all("a", href=pattern):
        href = anchor["href"]
        match = pattern.search(href)
        if not match:
            continue
        uuid = match.group(1)
        if uuid in seen:
            continue
        seen.add(uuid)

        # Extract just the filename — anchor text may contain a concatenated
        # format badge (e.g. "GTFS_FP2026_20260506.zipZIP") with no separator.
        raw = anchor.get_text()
        match_name = re.search(r"(GTFS_\S+?\.zip)", raw, re.IGNORECASE)
        if not match_name:
            continue
        name = match_name.group(1)

        # Build the direct download URL from the known pattern.
        filename_lower = name.lower()
        download_url = (
            f"https://data.opentransportdata.swiss/dataset/{DATASET_UUID}"
            f"/resource/{uuid}/download/{filename_lower}"
        )
        resources.append({"name": name, "uuid": uuid, "url": download_url})

    log.info("Found %d resource(s) on the dataset page.", len(resources))
    return resources


def _yyyymm_from_name(name: str) -> str:
    """Extract yyyymm from a filename like GTFS_FP2026_20260506.zip or GTFS_FP2026_2025-09-22.zip."""
    # Compact form: 8 consecutive digits (yyyymmdd)
    m = re.search(r"_(\d{8})\.", name)
    if m:
        return m.group(1)[:6]
    # Dashed form: yyyy-mm-dd
    m = re.search(r"_(\d{4})-(\d{2})-\d{2}\.", name)
    if m:
        return m.group(1) + m.group(2)
    raise ValueError(f"Cannot extract date from filename: {name}")


def download_file(session: requests.Session, url: str, dest: Path) -> None:
    """Stream-download *url* to *dest*, showing a simple progress indicator."""
    log.info("Downloading: %s", dest.name)
    with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        with open(dest, "wb") as fh:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    fh.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        print(
                            f"\r  {dest.name}: {downloaded / 1e6:.1f} MB"
                            f" / {total / 1e6:.1f} MB ({pct:.0f}%)",
                            end="",
                            flush=True,
                        )
    if total:
        print()  # newline after progress line
    log.info(
        "Downloaded %s  (%.1f MB)",
        dest.name,
        dest.stat().st_size / 1e6,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("GTFS downloader started at %s", datetime.now(timezone.utc).isoformat())
    log.info("MAX_FILES=%s  DATA_DIR=%s", MAX_FILES, DATA_DIR.resolve())

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    session = _make_session()

    try:
        resources = fetch_resource_list(session)
    except requests.RequestException as exc:
        log.error("Failed to fetch dataset page: %s", exc)
        sys.exit(1)

    if not resources:
        log.warning("No resources found — check if the page structure changed.")
        return

    # Apply the limit (resources are already newest-first).
    if MAX_FILES is not None:
        resources = resources[:MAX_FILES]
        log.info("Limiting to the %d most-recent file(s).", MAX_FILES)

    downloaded_count = 0
    skipped_count = 0
    error_count = 0

    for resource in resources:
        try:
            month_dir = DATA_DIR / _yyyymm_from_name(resource["name"])
        except ValueError as exc:
            log.warning("Skipping %s: %s", resource["name"], exc)
            error_count += 1
            continue
        month_dir.mkdir(parents=True, exist_ok=True)
        dest = month_dir / resource["name"]

        if dest.exists():
            log.info("SKIP  %s (already in %s)", resource["name"], DATA_DIR)
            skipped_count += 1
            continue

        try:
            download_file(session, resource["url"], dest)
            downloaded_count += 1
        except requests.RequestException as exc:
            log.error("ERROR downloading %s: %s", resource["name"], exc)
            # Remove any partial file so a re-run retries it cleanly.
            if dest.exists():
                dest.unlink()
            error_count += 1

    log.info(
        "Done — downloaded: %d  skipped: %d  errors: %d",
        downloaded_count,
        skipped_count,
        error_count,
    )
    log.info("=" * 60)


if __name__ == "__main__":
    main()
