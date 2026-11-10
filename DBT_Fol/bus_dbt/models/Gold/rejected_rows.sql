 
 with dup_rn as (select 
  *,
    row_number() over (partition by vehicle_id, event_ts
                               order by ingest_at asc)as rn
 from {{source('silver','Silver_Bus')}} 
 )
select vehicle_id, trip_id, route_id, event_ts, speed,
    latitude, longitude,
    `partition` as kafka_partition,
    `offset`    as kafka_offset,
 case when vehicle_id is null or event_ts is null then "null_key" end as reason
 from dup_rn 
 where rn >1 or vehicle_id is null or event_ts is null
 
 