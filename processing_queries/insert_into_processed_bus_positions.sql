-- incremental
insert into `warszawski-transport.warsaw_transport.processed_bus_positions`
with
data_spatially_filtered as (
  select
    pos.*
  from `warszawski-transport.warsaw_transport.preprocessed_bus_positions` pos
  left join `warszawski-transport.warsaw_transport.excluded_areas` as excl on ST_INTERSECTS(pos.geom, excl.geometry)
  where 1=1
    and partition_date = (CURRENT_DATE('Europe/Warsaw') - 1)
    and excl.geometry is null
),
data_with_prev_measurement as (
  select
    *,
    lag(position_datetime, 1) over prev_pos as prev_position_datetime,
    lag(geom, 1) over prev_pos as prev_geom
  from data_spatially_filtered
  window prev_pos as (partition by partition_date, line, brigade, vehicle_number order by position_datetime)
),
data_with_distance_and_time as (
  select
    *,
    cast(position_datetime as time) as position_time,
    time_diff(
      cast(position_datetime as time),
      cast(prev_position_datetime as time),
      SECOND
    ) as seconds_between_measurements,
    ST_DISTANCE(geom, prev_geom, true) as distance_m
  from data_with_prev_measurement
)
select
  partition_date,
  position_datetime,
  line,
  brigade,
  vehicle_number,
  geom,
  prev_position_datetime,
  prev_geom,
  seconds_between_measurements,
  distance_m,
  (distance_m / 1000) / (seconds_between_measurements / 3600) as speed_kmh
from data_with_distance_and_time
where seconds_between_measurements <= 60.0
;

-- backfill
truncate table `warszawski-transport.warsaw_transport.processed_bus_positions`;
insert into `warszawski-transport.warsaw_transport.processed_bus_positions`
with
data_spatially_filtered as (
  select
    pos.*
  from `warszawski-transport.warsaw_transport.preprocessed_bus_positions` pos
  left join `warszawski-transport.warsaw_transport.excluded_areas` as excl on ST_INTERSECTS(pos.geom, excl.geometry)
  where 1=1
    and partition_date <= (select max(partition_date) from `warszawski-transport.warsaw_transport.preprocessed_bus_positions`)
    and excl.geometry is null
),
data_with_prev_measurement as (
  select
    *,
    lag(position_datetime, 1) over prev_pos as prev_position_datetime,
    lag(geom, 1) over prev_pos as prev_geom
  from data_spatially_filtered
  window prev_pos as (partition by partition_date, line, brigade, vehicle_number order by position_datetime)
),
data_with_distance_and_time as (
  select
    *,
    cast(position_datetime as time) as position_time,
    time_diff(
      cast(position_datetime as time),
      cast(prev_position_datetime as time),
      SECOND
    ) as seconds_between_measurements,
    ST_DISTANCE(geom, prev_geom, true) as distance_m
  from data_with_prev_measurement
)
select
  partition_date,
  position_datetime,
  line,
  brigade,
  vehicle_number,
  geom,
  prev_position_datetime,
  prev_geom,
  seconds_between_measurements,
  distance_m,
  (distance_m / 1000) / (seconds_between_measurements / 3600) as speed_kmh
from data_with_distance_and_time
where seconds_between_measurements <= 60.0
;
