### Q1
````bash
docker build --help
```

### Q2
```bash
docker run -it --rm --entrypoint bash python:3.9
pip list
```

### Q3
```SQL
select count(*) from green_taxi_data where DATE(lpep_pickup_datetime) = '2019-01-15' and DATE(lpep_dropoff_datetime) = '2019-01-15';
```

### Q4
```SQL
select * from green_taxi_data order by trip_distance desc limit 1;
```

### Q5
```SQL
select count(*) from green_taxi_data where DATE(lpep_pickup_datetime) = '2019-01-01' and passenger_count = 2;
select count(*) from green_taxi_data where DATE(lpep_pickup_datetime) = '2019-01-01' and passenger_count = 3;
```

### Q6
```SQL
select * from zones
INNER JOIN
(
	select "DOLocationID", tip_amount from green_taxi_data where "PULocationID" = (
		select "LocationID" from zones where "Zone"='Astoria'
	)
	order by tip_amount desc limit 1
) as ps
ON zones."LocationID" = ps."DOLocationID";
```
