{{ config(materialized='table') }}

select
    event_date_ist,
    count(*)                                                              as row_count,
    count(distinct vehicle_id)                                            as buses,
    round(100.0 * avg(case when cal_speed_kmh is null then 1 else 0 end), 1) as pct_null_speed,
    round(100.0 * avg(case when is_gps_jumps then 1 else 0 end), 2)        as pct_gps_jump,
    round(100.0 * avg(case when dt_sec > 600 then 1 else 0 end), 1)       as pct_long_gaps,
    round(avg(unix_timestamp(ingest_at) - unix_timestamp(pulled_at)), 0)  as avg_feed_lag_sec   
from {{ ref('gold_bus') }}
group by event_date_ist