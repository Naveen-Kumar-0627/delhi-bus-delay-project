{{config(materialized='incremental',
incremental_strategy='merge',unique_key=['vehicle_id','event_ts'],
liquid_clustered_by=['event_date_ist','route_id']
)}}

 with base as (
select
        vehicle_id, trip_id, route_id, schedule_relationship,
        latitude, longitude, speed,
        event_ts, pulled_at, ingest_at,
        start_date, trip_start_ts,
        `partition` as kafka_partition,
        `offset`    as kafka_offset from
         {{ source('silver', 'Silver_Bus') }}
where vehicle_id is not null and
event_ts is  not null 
{%if is_incremental() %}
and ingest_at>(select max(ingest_at) from {{this}})- interval 1  hour

 {% endif %}
   
 )    ,
 dedup as (
    select * from base 
qualify row_number() over (partition by vehicle_id, event_ts
                               order by ingest_at asc) = 1

 ),
calc_lag as (

select
        vehicle_id, trip_id, route_id, schedule_relationship,
        latitude, longitude, speed,
        event_ts, pulled_at, ingest_at,
        start_date, trip_start_ts,
        kafka_partition,
       kafka_offset,
 lag(event_ts) over 
        (partition by vehicle_id  order by ingest_at) as prev_ts,
        lag(latitude) over 
        (partition by vehicle_id  order by ingest_at) as prev_lat,
        lag(longitude) over 
        (partition by vehicle_id  order by ingest_at) as prev_lon
        from dedup),
calc as (
    select vehicle_id, trip_id, route_id, schedule_relationship,
        latitude, longitude, speed,
        event_ts, pulled_at, ingest_at,
        start_date, trip_start_ts,
        unix_timestamp(event_ts) - unix_timestamp(prev_ts) as dt_sec,
        2 * 6371000 * asin(sqrt(
            pow(sin(radians(latitude - prev_lat) / 2), 2)
          + cos(radians(prev_lat)) * cos(radians(latitude))
            * pow(sin(radians(longitude - prev_lon) / 2), 2)
        )) as dist_m
    from calc_lag
)
select * ,
from_utc_timestamp(event_ts,"Asia/Kolkata") as event_ts_ist,
 to_date(from_utc_timestamp(event_ts,"Asia/Kolkata")) as event_date_ist,
 hour(from_utc_timestamp(event_ts,"Asia/Kolkata")) as event_hour_ist ,
case when dt_sec between 1 and 600 and dist_m <10 then 0.0
when dt_sec between 1 and 600 and
 dist_m/dt_sec * 3.6 <=100 then round(dist_m/dt_sec * 3.6,0) 
 end as cal_speed_kmh,
 coalesce(dt_sec between 1 and 600 and dist_m/dt_sec * 3.6> 100,false) as is_gps_jumps
 from calc
 {% if is_incremental() %}
 where ingest_at > (select max(ingest_at) from {{this}})

{% endif %}