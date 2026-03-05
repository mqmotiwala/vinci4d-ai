"""
noaa_pipeline.py
----------------
Fetches daily TMAX, TMIN, PRCP from NOAA CDO /data endpoint for each
selected station and writes raw responses to S3.

S3 layout:
    {bucket}/raw/city_id={slug}/year={yyyy}/month={mm}/day={dd}/data.json

Each data.json accumulates records across all stations for that city/date:
    {
        "city_slug":     "san-francisco",
        "date":          "2024-03-04",
        "pulled_at":     "2026-03-05T08:00:00Z",
        "station_count": 3,
        "results": [
            {"date": "...", "datatype": "TMAX", "station": "GHCND:...", "value": 72},
            ...
        ]
    }

Usage:
    python noaa_pipeline.py                         # last 5 years, all cities
    python noaa_pipeline.py --start 2024-01-01      # custom start date
    python noaa_pipeline.py --city san-francisco    # single city by slug
"""

# load dependencies
import time
import argparse
import config as c
import urllib.error
import utils.helpers as h
from datetime import date, datetime, timezone
from utils.logger import logger

# see ReadMe.md for selection methodology
CITY_STATIONS = {
    "san-francisco": [
        "GHCND:USW00023272",   # SF Downtown — 45m
        "GHCND:USW00023234",   # SFO Airport — 3m
        "GHCND:USC00044500",   # Kentfield — 44m
        "GHCND:USC00043714",   # Half Moon Bay — 8m
        "GHCND:USC00047414",   # Richmond — 6m
        "GHCND:USC00046336",   # Oakland Museum — 9m
    ],
    "san-jose": [
        "GHCND:USW00023293",   # San Jose — 15m
        "GHCND:USW00093228",   # Hayward Air Terminal — 9m
        "GHCND:USC00043244",   # Fremont — 11m
    ],
    "sacramento": [
        "GHCND:USW00093225",   # Sacramento Metropolitan Airport — 7.5m
        "GHCND:USW00023232",   # Sacramento Airport ASOS — 5.9m
    ],
    "santa-cruz": [
        "GHCND:USW00023277",   # Watsonville Municipal Airport — 49m
        "GHCND:USC00040673",   # Ben Lomond Number 4 — 132m
        "GHCND:USC00049473",   # Watsonville Waterworks — 29m
    ],
}

# NOAA API config
DATASET_ID = "GHCND"
DATATYPES = "TMAX,TMIN,PRCP"
NOAA_DATA_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

YEARS_BACK = 5
PAGE_LIMIT = 1000
RATE_LIMIT_S = 0.25     # NOAA allows ~5 req/s
CHUNK_DAYS = 365        # NOAA rejects ranges longer than 1 year per request

def fetch_station_chunk(station_id, start, end):
    """
    Fetches all paginated NOAA records for a single station over [start, end].
    Returns a flat list of result dicts.
    """
    all_results = []
    offset = 1

    while True:
        params = {
            "datasetid": DATASET_ID,
            "stationid": station_id,
            "datatypeid": DATATYPES,
            "startdate": start.isoformat(),
            "enddate": end.isoformat(),
            "units": "standard",
            "limit": PAGE_LIMIT,
            "offset": offset,
        }

        try:
            resp = h.noaa_get(NOAA_DATA_URL, c.NOAA_TOKEN, params)
        except urllib.error.HTTPError as e:
            logger.error(f"[HTTP {e.code}] {station_id} offset={offset}: {e.reason}")
            break
        except Exception as e:
            logger.error(f"Exception fetching {station_id} offset={offset}: {e}")
            break

        results = resp.get("results") or []
        all_results.extend(results)

        count = resp.get("metadata", {}).get("resultset", {}).get("count", 0)
        logger.info(f"{station_id} offset={offset} → {len(results)} records (total={count})")

        if offset + PAGE_LIMIT > count:
            break

        offset += PAGE_LIMIT
        time.sleep(RATE_LIMIT_S)

    return all_results

def write_days(city_slug, results, pulled_at):
    """
    Groups a flat list of records (from all stations for a city) by calendar
    date and writes one data.json per day to S3.
    Returns count of day-files written.
    """
    by_date = h.group_results_by_date(results)

    written = 0
    for day_str, day_results in sorted(by_date.items()):
        d   = date.fromisoformat(day_str)
        key = h.s3_key(city_slug, d)

        payload = {
            "city_slug": city_slug,
            "date": day_str,
            "pulled_at": pulled_at,
            "station_count": len(set(r["station"] for r in day_results)),
            "results": day_results,
        }
        
        logger.info(f"Writing {len(day_results)} records for {city_slug}/{day_str}")
        h.s3_write(c.s3, c.S3_BUCKET, key, payload)
        written += 1

    return written

def run(city_filter=None, start=None, end=None, lookback_days=7):
    today = date.today()
    start = start or date(today.year - YEARS_BACK, today.month, today.day)
    end = end or today
    pulled_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    cities = (
        {city_filter: CITY_STATIONS[city_filter]}
        if city_filter
        else CITY_STATIONS
    )

    for city_slug, stations in cities.items():
        logger.info(f"City: {city_slug} | stations: {len(stations)} | range: {start} → {end}")

        needed = h.dates_to_backfill(c.s3, c.S3_BUCKET, city_slug, start, end, lookback_days)
        logger.info(f"{city_slug}: {len(needed)} dates to fetch")

        if not needed:
            logger.info(f"{city_slug}: nothing to do — all dates present and outside lookback window")
            continue

        total_written = 0

        for chunk_start, chunk_end in h.chunk_date_range(start, end, CHUNK_DAYS):
            chunk_dates = {d.isoformat() for d in h.date_range(chunk_start, chunk_end)}
            if not (needed & chunk_dates):
                continue

            logger.info(f"{city_slug}: chunk {chunk_start} → {chunk_end}")

            chunk_results = []
            for station_id in stations:
                logger.info(f"{city_slug}: fetching {station_id}")
                results = fetch_station_chunk(station_id, chunk_start, chunk_end)
                chunk_results.extend(results)
                time.sleep(RATE_LIMIT_S)

            logger.info(f"{city_slug}: {len(chunk_results)} total records across {len(stations)} station(s)")

            if not chunk_results:
                logger.warning(f"{city_slug}: no data returned for chunk {chunk_start} → {chunk_end} — skipping")
                continue

            written = write_days(city_slug, chunk_results, pulled_at)
            total_written += written
            logger.info(f"{city_slug}: {written} day-files written to S3")

        logger.info(f"{city_slug}: done — {total_written} total day-files written")

    logger.info("Pipeline run complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NOAA CDO → S3 pipeline")
    
    parser.add_argument(
        "--city",
        help="Single city slug to run, e.g. san-francisco. Defaults to all.",
        default=None,
        choices=list(CITY_STATIONS.keys()),
    )

    parser.add_argument(
        "--start",
        help="Start date YYYY-MM-DD. Defaults to 5 years ago.",
        default=None,
    )
    
    parser.add_argument(
        "--end",
        help="End date YYYY-MM-DD. Defaults to today.",
        default=None,
    )

    args = parser.parse_args()

    run(
        city_filter=args.city,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
    )