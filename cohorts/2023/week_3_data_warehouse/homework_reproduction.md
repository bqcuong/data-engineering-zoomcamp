### Setup
```bash
python etl_fhv_to_gs.py
```

```SQL
CREATE OR REPLACE EXTERNAL TABLE bg_taxi2.external_fhv_trips
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoomcamp-taxi/data/fhv_tripdata_2019-*.csv.gz']
);
```

### Q1
```SQL
SELECT count(*) FROM bg_taxi2.external_fhv_trips;
```

### Q2
```SQL
CREATE OR REPLACE TABLE bg_taxi2.fhv_trips AS
SELECT * FROM bg_taxi2.external_fhv_trips;

SELECT DISTINCT(affiliated_base_number) FROM bg_taxi2.fhv_trips;
SELECT DISTINCT(affiliated_base_number) FROM bg_taxi2.external_fhv_trips;
```

### Q3
```SQL
SELECT COUNT(*) FROM bg_taxi2.fhv_trips WHERE PUlocationID IS NULL and DOlocationID IS NULL;
```

### Q4
```SQL
CREATE OR REPLACE TABLE bg_taxi2.fhv_trips_partitioned_clustered
PARTITION BY DATE(pickup_datetime) 
CLUSTER BY affiliated_base_number
AS SELECT * FROM bg_taxi2.external_fhv_trips;
```

### Q5
```SQL
SELECT DISTINCT(affiliated_base_number) FROM bg_taxi2.fhv_trips
  WHERE pickup_datetime >= '2019-03-01' AND pickup_datetime <= '2019-03-31';

SELECT DISTINCT(affiliated_base_number) FROM bg_taxi2.fhv_trips_partitioned_clustered
  WHERE pickup_datetime >= '2019-03-01' AND pickup_datetime <= '2019-03-31';
```