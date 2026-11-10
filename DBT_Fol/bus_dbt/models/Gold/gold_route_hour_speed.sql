{{ config(materialized='table') }}

select
    route_id,
    event_hour_ist                                          as hour_ist,
    count(*)                                                as observations,
    count(distinct vehicle_id)                              as buses,
    round(avg(cal_speed_kmh), 1)                            as avg_kmh,
    round(percentile_approx(cal_speed_kmh, 0.5), 1)         as median_kmh,
    round(avg(case when cal_speed_kmh = 0 then 1.0 else 0 end), 2) as share_stopped
from {{ ref('gold_bus') }}
where cal_speed_kmh is not null
  and not is_gps_jumps
group by route_id, event_hour_ist
having count(*) >= 30           