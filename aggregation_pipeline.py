"""
aggregation_pipeline.py
-----------------------
Reads raw daily data.json files from S3, averages TMAX, TMIN, PRCP
across all reporting stations per city/date, and writes one CSV per
city/year to the aggregated layer.

S3 input:
    raw/city_id={slug}/year={yyyy}/month={mm}/day={dd}/data.json

S3 output:
    aggregated/city_id={slug}/year={yyyy}/data.csv

CSV columns:
    city_slug, date, tmax_avg, tmin_avg, prcp_avg, station_count

Rules:
    - Each datatype is averaged independently across all stations that
      reported it on that date. A station does not need to report all
      three to contribute to any one average.
    - If no station reported a datatype on a given date, the value is
      written as empty (null).
    - station_count reflects the number of distinct stations that
      reported at least one datatype on that date.

Usage:
    python aggregation_pipeline.py   # all cities, all years
    python aggregation_pipeline.py --city san-francisco
    python aggregation_pipeline.py --year 2024
    python aggregation_pipeline.py --city sacramento --year 2023
"""

import argparse
import csv
import io
import json
from datetime import date

import config as c
import utils.helpers as h
from utils.logger import logger


YEARS_BACK = 5
CITY_SLUGS = ["san-francisco", "san-jose", "sacramento", "santa-cruz"]


def list_raw_keys(city_slug, year):
    """
    Returns a list of all raw data.json S3 keys for a given city/year.
    """
    prefix = f"raw/city_id={city_slug}/year={year}/"
    keys = []
    paginator = c.s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=c.S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents") or []:
            if obj["Key"].endswith("data.json"):
                keys.append(obj["Key"])
    return keys


def read_raw(key):
    """Reads and parses a raw data.json from S3."""
    resp = c.s3.get_object(Bucket=c.S3_BUCKET, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def write_aggregated_csv(city_slug, year, rows):
    """
    Serializes a list of aggregated row dicts to CSV and writes to S3.
    rows: list of dicts with keys city_slug, date, tmax_avg, tmin_avg,
          prcp_avg, station_count — sorted by date.
    """
    key = f"aggregated/city_id={city_slug}/year={year}/data.csv"
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=["city_slug", "date", "tmax_avg", "tmin_avg", "prcp_avg", "station_count"],
    )
    writer.writeheader()
    writer.writerows(rows)

    c.s3.put_object(
        Bucket=c.S3_BUCKET,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info(f"Wrote {len(rows)} rows -> s3://{c.S3_BUCKET}/{key}")


def aggregate_day(raw):
    """
    Given a raw data.json payload, compute per-datatype averages across
    all stations that reported on that date.

    Returns a dict:
        {
            "tmax_avg": float | None,
            "tmin_avg": float | None,
            "prcp_avg": float | None,
            "station_count": int,
        }
    """
    tmax_vals = []
    tmin_vals = []
    prcp_vals = []

    for rec in raw.get("results") or []:
        val = rec.get("value")
        if val is None:
            continue
        dt = rec.get("datatype")
        if dt == "TMAX":
            tmax_vals.append(val)
        elif dt == "TMIN":
            tmin_vals.append(val)
        elif dt == "PRCP":
            prcp_vals.append(val)

    station_count = len(set(
        r["station"] for r in raw.get("results") or []
        if r.get("station")
    ))

    def avg(vals):
        if not vals:
            return None
        return round(sum(vals) / len(vals), 2)

    return {
        "tmax_avg": avg(tmax_vals),
        "tmin_avg": avg(tmin_vals),
        "prcp_avg": avg(prcp_vals),
        "station_count": station_count,
    }


def run(city_filter=None, year_filter=None):
    today = date.today()
    years = (
        [year_filter]
        if year_filter
        else list(range(today.year - YEARS_BACK, today.year + 1))
    )
    cities = [city_filter] if city_filter else CITY_SLUGS

    for city_slug in cities:
        for year in years:
            logger.info(f"{city_slug}/{year}: starting aggregation")

            keys = list_raw_keys(city_slug, year)
            if not keys:
                logger.warning(f"{city_slug}/{year}: no raw files found, skipping")
                continue

            logger.info(f"{city_slug}/{year}: {len(keys)} raw files found")

            rows = []
            for key in sorted(keys):
                try:
                    raw = read_raw(key)
                except Exception as e:
                    logger.error(f"Failed to read {key}: {e}")
                    continue

                agg = aggregate_day(raw)
                rows.append({
                    "city_slug": city_slug,
                    "date": raw["date"],
                    "tmax_avg": agg["tmax_avg"] if agg["tmax_avg"] is not None else "",
                    "tmin_avg": agg["tmin_avg"] if agg["tmin_avg"] is not None else "",
                    "prcp_avg": agg["prcp_avg"] if agg["prcp_avg"] is not None else "",
                    "station_count": agg["station_count"],
                })

            write_aggregated_csv(city_slug, year, rows)
            logger.info(f"{city_slug}/{year}: done - {len(rows)} days aggregated")

    logger.info("Aggregation complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Raw -> aggregated pipeline")
    parser.add_argument(
        "--city",
        help="Single city slug to run. Defaults to all.",
        default=None,
        choices=CITY_SLUGS,
    )
    parser.add_argument(
        "--year",
        help="Single year to run e.g. 2024. Defaults to all years.",
        type=int,
        default=None,
    )
    args = parser.parse_args()

    run(
        city_filter=args.city,
        year_filter=args.year,
    )