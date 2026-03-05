"""
aggregation_pipeline.py
-----------------------
Reads raw daily data.json files from S3, averages TMAX, TMIN, PRCP
across all reporting stations for that city/date, and writes one
aggregated data.json per city/date.

S3 input:
    raw/city_id={slug}/year={yyyy}/month={mm}/day={dd}/data.json

S3 output:
    aggregated/city_id={slug}/year={yyyy}/month={mm}/day={dd}/data.json

Output payload:
    {
        "city_slug": "san-francisco",
        "date": "2024-03-04",
        "tmax_avg": 61.2,
        "tmin_avg": 45.1,
        "prcp_avg": 0.04,
        "station_count": 4
    }

Each datatype is averaged independently — a station does not need to
report all three to contribute. If no station reported a datatype on
a given date, that field is null.

Usage:
    python aggregation_pipeline.py                  # all cities, all years
    python aggregation_pipeline.py --city san-francisco
    python aggregation_pipeline.py --year 2024
    python aggregation_pipeline.py --city sacramento --year 2023
"""

import argparse
from datetime import date

import config as c
import utils.helpers as h
from utils.logger import logger


YEARS_BACK = 5
CITY_SLUGS = ["san-francisco", "san-jose", "sacramento", "santa-cruz"]


def aggregate_day(raw):
    """
    Given a raw data.json payload, average each datatype independently
    across all stations that reported a value on that date.

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

            keys = h.list_raw_keys(c.s3, c.S3_BUCKET, city_slug, year)
            if not keys:
                logger.warning(f"{city_slug}/{year}: no raw files found, skipping")
                continue

            logger.info(f"{city_slug}/{year}: {len(keys)} raw files found")

            written = 0
            for key in keys:
                raw = h.s3_read_json(c.s3, c.S3_BUCKET, key)
                if not raw:
                    logger.error(f"Failed to read {key}, skipping")
                    continue

                agg = aggregate_day(raw)
                payload = {
                    "city_slug": city_slug,
                    "date": raw["date"],
                    "tmax_avg": agg["tmax_avg"],
                    "tmin_avg": agg["tmin_avg"],
                    "prcp_avg": agg["prcp_avg"],
                    "station_count": agg["station_count"],
                }

                d = date.fromisoformat(raw["date"])
                out_key = h.aggregated_s3_key(city_slug, d)
                h.s3_write(c.s3, c.S3_BUCKET, out_key, payload)
                written += 1

            logger.info(f"{city_slug}/{year}: done - {written} days written")

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