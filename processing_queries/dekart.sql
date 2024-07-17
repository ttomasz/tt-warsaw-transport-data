select
  time_bucket_1h,
  h3_z11,
  (
    sum(case when speed_kmh < 5.0 then 1 else 0 end) / count(*)
  ) as percent_measurements_below_5kmh,
  count(*) as number_of_measurements
from `warszawski-transport.warsaw_transport.processed_bus_positions`
group by 1,2
having count(*) > 2
;

select
  concat(partition_date, 'T', time_bucket_15min) as time_bucket,
  h3_z11,
  (
    sum(case when speed_kmh < 5.0 then 1 else 0 end) / count(*)
  ) as percent_measurements_below_5kmh,
  count(*) as number_of_measurements
from `warszawski-transport.warsaw_transport.processed_bus_positions`
group by 1,2
having count(*) > 2
;


select
  concat('1970-01-01', 'T', time_bucket_5min) as time_bucket,
  h3_z11,
  (
    sum(case when speed_kmh < 5.0 then 1 else 0 end) / count(*)
  ) as percent_measurements_below_5kmh,
  count(*) as number_of_measurements
from `warszawski-transport.warsaw_transport.enriched_bus_positions`
--where time_bucket_5min between '05:00:00' and '21:00:00'
group by 1,2
having count(*) > 10
;



with
positions as (
  select
    time_bucket_15min,
    line,
    brigade,
    vehicle_number,
    position_datetime,
    json_array(st_x(geom), st_y(geom), 0, unix_seconds(cast(position_datetime as timestamp))) as coord
  from `warszawski-transport.warsaw_transport.enriched_bus_positions`
  where partition_date = '2024-07-11'
)
select *
from positions
order by     line,
    brigade,
    vehicle_number,
    position_datetime
;

with
positions as (
  select
    time_bucket_15min,
    line,
    brigade,
    vehicle_number,
    position_datetime,
    speed_kmh,
    json_array(st_x(geom), st_y(geom), 0, unix_seconds(cast(position_datetime as timestamp))) as coord
  from `warszawski-transport.warsaw_transport.enriched_bus_positions`
  where partition_date = '2024-07-11'
)
select *
from positions
order by
  line,
  brigade,
  vehicle_number,
  position_datetime
;
