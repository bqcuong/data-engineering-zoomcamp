### Setup
```bash
python etl_taxitrips_to_gs.py
```

### Q1
```SQL
SELECT COUNT(*) FROM production.fact_trips WHERE pickup_datetime >= '2019-01-01' and pickup_datetime <= '2020-12-31'
```

### Q2
```
# no commands
```

### Q3
```SQL
SELECT COUNT(*) FROM production.stg_fhv_tripdata WHERE pickup_datetime >= '2019-01-01' and pickup_datetime <= '2019-12-31'
```

### Q4
```SQL
SELECT COUNT(*) FROM production.fact_fhv_trips WHERE pickup_datetime >= '2019-01-01' and pickup_datetime <= '2019-12-31'
```

### Q5
```
no commands
```