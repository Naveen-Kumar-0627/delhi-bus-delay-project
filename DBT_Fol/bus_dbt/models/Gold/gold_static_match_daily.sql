{{ config(materialized='table') }}

with live as (
    select distinct event_ts, route_id, trip_id from 
      {{ source('silver', 'Silver_Bus') }}   
)
select
    date(l.event_ts) as event_date,
    count(distinct l.route_id)                                              as live_routes,
    count(distinct case when r.route_id is not null then l.route_id end)    as routes_in_static,
    round(100.0 * count(distinct case when r.route_id is not null then l.route_id end)
          / count(distinct l.route_id), 1)                                  as route_match_pct,
    count(distinct l.trip_id)                                               as live_trips,
    count(distinct case when t.trip_id is not null then l.trip_id end)      as trips_in_static,
    round(100.0 * count(distinct case when t.trip_id is not null then l.trip_id end)
          / count(distinct l.trip_id), 1)                                   as trip_match_pct
from live l
left join {{ source('bronze', 'bronze_routes') }} r on l.route_id = r.route_id
left join {{ source('bronze', 'bronze_trips')  }} t on l.trip_id  = t.trip_id
group by date(l.event_ts)