{{ config(materialized='table') }}

select event_date_ist, event_hour_ist, route_id,
       count(distinct vehicle_id) as active_buses
from {{ ref('gold_bus') }}
group by event_date_ist, event_hour_ist, route_id