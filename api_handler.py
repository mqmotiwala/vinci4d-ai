"""
api_handler.py
----------
AWS Lambda handler for the Climate Dashboard API.

Routes:
    GET /weather/{city_slug}

For each of the 5 forecast days, the handler:
    1. Calls weather.gov for the current forecast
    2. Reads the aggregated data.json for the same month/day across
       the previous 5 years from S3
    3. Averages available values to produce historical context
    4. Returns forecast + historical averages combined

Response shape:
    {
        "city": "san-francisco",
        "generated_at": "2026-03-05T08:00:00Z",
        "forecast": [
            {
                "date": "2026-03-05",
                "high_temp_f": 63.2,
                "low_temp_f": 47.8,
                "short_forecast": "Partly Cloudy",
                "detailed_forecast": "...",
                "wind_speed": "10 mph",
                "wind_direction": "W",
                "precip_chance_pct": 22,
                "historical_avg": {
                    "tmax_avg": 62.6,
                    "tmin_avg": 45.2,
                    "prcp_avg": 0.108,
                    "years_available": 4
                }
            },
            ...
        ]
    }
"""

import json
import urllib.request
from datetime import date, datetime, timezone

import boto3

S3_BUCKET = "vinci-676206945006"
s3 = boto3.client("s3")

# City registry
# WFO grid coordinates sourced from api.weather.gov/points/{lat},{lon}
CITIES = {
    "san-francisco": {"wfo": "MTR", "grid_x": 85, "grid_y": 106},
    "san-jose":      {"wfo": "MTR", "grid_x": 99,  "grid_y": 82},
    "sacramento":    {"wfo": "STO", "grid_x": 41,  "grid_y": 68},
    "santa-cruz":    {"wfo": "MTR", "grid_x": 91, "grid_y": 67},
}

WX_BASE = "https://api.weather.gov"
WX_USER_AGENT = "(climate-dashboard, mqmotiwala@gmail.com)"
YEARS_BACK = 5

def fetch_forecast(city_slug):
    """
    Fetches the NWS forecast periods for a city.
    Returns a list of period dicts from the weather.gov API.
    """
    city = CITIES[city_slug]
    url = f"{WX_BASE}/gridpoints/{city['wfo']}/{city['grid_x']},{city['grid_y']}/forecast"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": WX_USER_AGENT,
            "Accept": "application/geo+json",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data["properties"]["periods"]


def parse_precip_chance(period):
    """
    Extracts precipitation probability from a forecast period.
    Uses the probabilityOfPrecipitation field if available,
    falls back to parsing the detailed forecast text.
    """
    prob = period.get("probabilityOfPrecipitation", {})
    if prob and prob.get("value") is not None:
        return prob["value"]

    import re
    text = period.get("detailedForecast", "")
    m = re.search(r"(\d+)\s*percent chance", text, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"chance of precipitation is (\d+)%", text, re.I)
    if m:
        return int(m.group(1))
    return None


def build_forecast_days(periods):
    """
    Collapses NWS day/night period pairs into one dict per calendar date.
    Returns a list of dicts sorted by date, limited to 5 days.
    """
    today = date.today()
    by_date = {}

    for period in periods:
        period_date = period["startTime"][:10]
        if period_date not in by_date:
            by_date[period_date] = {}
        if period.get("isDaytime"):
            by_date[period_date]["day"] = period
        else:
            by_date[period_date]["night"] = period

    days = []
    for d in sorted(by_date)[:5]:
        day_period = by_date[d].get("day") or {}
        night_period = by_date[d].get("night") or {}
        days.append({
            "date": d,
            "high_temp_f": day_period.get("temperature"),
            "low_temp_f": night_period.get("temperature"),
            "short_forecast": day_period.get("shortForecast", ""),
            "detailed_forecast": day_period.get("detailedForecast", ""),
            "wind_speed": day_period.get("windSpeed", ""),
            "wind_direction": day_period.get("windDirection", ""),
            "precip_chance_pct": parse_precip_chance(day_period),
        })

    return days


def aggregated_s3_key(city_slug, d):
    return (
        f"aggregated/city_id={city_slug}"
        f"/year={d.year}"
        f"/month={d.month:02d}"
        f"/day={d.day:02d}"
        f"/data.json"
    )


def read_aggregated(city_slug, d):
    """
    Reads the aggregated data.json for a city/date from S3.
    Returns None if the file does not exist.
    """
    key = aggregated_s3_key(city_slug, d)
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception:
        return None


def historical_avg(city_slug, forecast_date_str):
    """
    For a given forecast date, reads the aggregated file for the same
    month/day across the previous YEARS_BACK years and averages the
    available values.

    Skips any year where the file doesn't exist or a value is null —
    uses as many years of data as are available.

    Returns a dict:
        {
            "tmax_avg": float | None,
            "tmin_avg": float | None,
            "prcp_avg": float | None,
            "years_available": int,
        }
    """
    forecast_date = date.fromisoformat(forecast_date_str)
    tmax_vals = []
    tmin_vals = []
    prcp_vals = []
    years_available = 0

    for years_ago in range(1, YEARS_BACK + 1):
        prior_date = forecast_date.replace(year=forecast_date.year - years_ago)
        data = read_aggregated(city_slug, prior_date)
        if not data:
            continue
        years_available += 1
        if data.get("tmax_avg") is not None:
            tmax_vals.append(data["tmax_avg"])
        if data.get("tmin_avg") is not None:
            tmin_vals.append(data["tmin_avg"])
        if data.get("prcp_avg") is not None:
            prcp_vals.append(data["prcp_avg"])

    def avg(vals):
        if not vals:
            return None
        return round(sum(vals) / len(vals), 2)

    return {
        "tmax_avg": avg(tmax_vals),
        "tmin_avg": avg(tmin_vals),
        "prcp_avg": avg(prcp_vals),
        "years_available": years_available,
    }


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    # Parse city slug from path /weather/{city_slug}
    path = event.get("rawPath") or event.get("path", "")
    parts = [p for p in path.strip("/").split("/") if p]

    # normalize path to always start from the route
    if "weather" in parts:
        parts = parts[parts.index("weather"):]
        
    if len(parts) != 2 or parts[0] != "weather":
        return response(404, {"error": "Not found. Expected /weather/{city_slug}"})

    city_slug = parts[1]
    if city_slug not in CITIES:
        return response(404, {
            "error": f"Unknown city: {city_slug}",
            "valid_cities": list(CITIES.keys()),
        })

    # Fetch forecast from weather.gov
    try:
        periods = fetch_forecast(city_slug)
    except Exception as e:
        return response(502, {"error": f"Failed to fetch forecast: {str(e)}"})

    forecast_days = build_forecast_days(periods)

    # Attach historical averages to each forecast day
    for day in forecast_days:
        day["historical_avg"] = historical_avg(city_slug, day["date"])

    return response(200, {
        "city": city_slug,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forecast": forecast_days,
    })