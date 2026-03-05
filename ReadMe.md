# Callouts
## `noaa_pipeline`
I originally designed this pipeline to make climate data requests granularized at the city level, with the intent of aggregating results across all stations that reported from this city.  

But, for large cities, like in the assignment, there are a large number of stations reporting data from a variety of elevations, and microclimates (esp. true for San Francisco!). 

It wouldn't make sense to average across all those stations, since that would produce averaged numbers that don't represent any real place in these cities. 

So, I used the noaa API's /stations endpoint to hand-pick select stations from each city to retrieve data from. The selection criteria was roughly: 
- `datacoverage` >= 0.85 (reliable reporters)
- `maxdate` within the last 30-60 days (still active)  
- `mindate` before your 5-year start date (full coverage in our range)
- elevation relatively appropriate

Example API call params:

```
locationid:CITY:US060037
limit:1000
datasetid:GHCND
enddate:2026-01-01
startdate:2021-03-05
datatypeid:TMAX,TMIN,PRCP
sortfield:datacoverage
sortorder:desc
```

--

I'm making a decision to not work with CSVs at all in this pipeline.  
My pipelines will generate the necessary data for my API to serve to the frontend.  
Any UI requirements, like serving the data in tabular format, should be met by the frontend team by parsing a json payload.  
This allows for leaner pipelines, and I believe it's the right call to make.  

--

## API Design

The API exposes a single route:
```
GET /weather/{city_slug}
```

The request returns everything the dashboard needs for a given city. On each invocation the handler performs two operations:

1. **Forecast** — calls the NWS weather.gov API in real time for the requested city's 5-day forecast. No forecast data is stored; it is always fetched fresh so the response reflects the latest NWS update.

2. **Historical averages** — for each of the 5 forecast dates, the handler derives the same calendar date across the previous 5 years and reads the corresponding pre-aggregated daily files from S3. It then averages the available values on the fly to produce historical context for that date.

This means historical averages are computed at request time rather than pre-computed and stored. This is intentional — as the NOAA aggregation pipeline runs and backfills or corrects data, the historical averages automatically reflect the latest state without any additional pipeline steps.

The S3 aggregated layer follows the partition structure:
```
aggregated/city_id={slug}/year={yyyy}/month={mm}/day={dd}/data.json
```

Each file contains the station-averaged TMAX, TMIN, and PRCP for that city on that date. The API reads up to 5 of these files per forecast day (one per prior year), skipping any that are missing or incomplete, and averages whatever is available.