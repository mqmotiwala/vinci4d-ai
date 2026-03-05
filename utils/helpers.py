"""
helpers.py
----------
Shared utilities for the climate data pipelines.
"""

import json
import urllib.parse
import urllib.request
from datetime import date, timedelta


def noaa_get(base_url, token, params):
    """
    Authenticated GET request to the NOAA CDO API.
    Returns parsed JSON dict. Returns empty results dict on empty body.
    """
    qs = urllib.parse.urlencode(params)
    url = f"{base_url}?{qs}"
    req = urllib.request.Request(url, headers={"token": token})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    if not raw.strip():
        return {"metadata": {}, "results": []}
    return json.loads(raw)


def raw_s3_key(city_slug, d):
    """
    Canonical S3 key for a raw data.json given a city slug and date object.
    raw/city_id=san-francisco/year=2024/month=03/day=04/data.json
    """
    return (
        f"raw/city_id={city_slug}"
        f"/year={d.year}"
        f"/month={d.month:02d}"
        f"/day={d.day:02d}"
        f"/data.json"
    )


def aggregated_s3_key(city_slug, d):
    """
    Canonical S3 key for an aggregated data.json given a city slug and date object.
    aggregated/city_id=san-francisco/year=2024/month=03/day=04/data.json
    """
    return (
        f"aggregated/city_id={city_slug}"
        f"/year={d.year}"
        f"/month={d.month:02d}"
        f"/day={d.day:02d}"
        f"/data.json"
    )


def s3_write(s3_client, bucket, key, payload):
    """Serialize payload to JSON and write to S3. Always overwrites."""
    body = json.dumps(payload, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


def s3_read_json(s3_client, bucket, key):
    """
    Read and parse a JSON file from S3.
    Returns None if the key does not exist or on any error.
    """
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception:
        return None


def list_raw_keys(s3_client, bucket, city_slug, year):
    """
    Returns a sorted list of all raw data.json S3 keys for a given city/year.
    """
    prefix = f"raw/city_id={city_slug}/year={year}/"
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            if obj["Key"].endswith("data.json"):
                keys.append(obj["Key"])
    return sorted(keys)


def list_existing_dates(s3_client, bucket, city_slug, year):
    """
    Returns a set of "YYYY-MM-DD" strings already present in S3
    for a given city slug and year.
    """
    prefix = f"raw/city_id={city_slug}/year={year}/"
    existing = set()
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes") or []:
            month_prefix = cp["Prefix"]
            for page2 in paginator.paginate(Bucket=bucket, Prefix=month_prefix, Delimiter="/"):
                for dp in page2.get("CommonPrefixes") or []:
                    # ".../year=2024/month=03/day=04/"
                    parts = dp["Prefix"].rstrip("/").split("/")
                    month = parts[-2].split("=")[1]
                    day = parts[-1].split("=")[1]
                    existing.add(f"{year}-{month}-{day}")

    return existing


def date_range(start, end):
    """Yield every date from start to end inclusive."""
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def chunk_date_range(start, end, chunk_days=365):
    """
    Yield (chunk_start, chunk_end) pairs covering [start, end]
    with no single chunk exceeding chunk_days.
    """
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def dates_to_backfill(s3_client, bucket, city_slug, start, end, lookback_days):
    """
    Returns the set of ISO date strings that need to be (re)fetched:
      - any date in [start, end] not yet present in S3
      - any date within the last lookback_days (re-pull for NOAA corrections)
    """
    all_dates = {d.isoformat() for d in date_range(start, end)}
    recent_cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    recent = {d for d in all_dates if d >= recent_cutoff}

    existing = set()
    for year in range(start.year, end.year + 1):
        existing |= list_existing_dates(s3_client, bucket, city_slug, year)

    missing = all_dates - existing
    return missing | recent


def group_results_by_date(results):
    """
    Groups a flat list of NOAA records by calendar date.
    Returns a dict of {date_str: [records]}.
    """
    by_date = {}
    for rec in results:
        day_str = rec["date"][:10]   # "2024-03-04T00:00:00" -> "2024-03-04"
        if day_str not in by_date:
            by_date[day_str] = []
        by_date[day_str].append(rec)
    return by_date