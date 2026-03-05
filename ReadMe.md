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
Any UI requirements, like serving the data in tabular format, can be met by the frontend team.  
This allows for leaner pipelines, I believe it's the right call to make.  