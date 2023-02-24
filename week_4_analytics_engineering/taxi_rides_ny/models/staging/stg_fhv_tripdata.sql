{{ config(materialized='view') }}

with tripdata as 
(
  select *  from {{ source('staging','fhv_tripdata') }}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime', 'dropOff_datetime']) }} as tripid,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    dispatching_base_num,
    Affiliated_base_number as affiliated_base_num
    
from tripdata
