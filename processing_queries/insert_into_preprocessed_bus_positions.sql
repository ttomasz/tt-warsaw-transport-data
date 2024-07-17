-- incremental
insert into `warszawski-transport.warsaw_transport.preprocessed_bus_positions`
with
data as (
  select
    position_datetime,
    line,
    brigade,
    vehicle_number,
    longitude,
    latitude
  from warsaw_transport.raw_bus_positions
  where 1=1
    and ingestion_date = (CURRENT_DATE('Europe/Warsaw') - 1)
    and (request_time_local - position_datetime) < MAKE_INTERVAL(minute => 5)
    and cast(position_datetime as date) = (CURRENT_DATE('Europe/Warsaw') - 1)
    and longitude between 20.0 and 22.0  -- remove coordinates outside warsaw metropolitan area
    and latitude between 51.0 and 53.0
    and line not in ('0w', '0z')  -- remove bus lines that are not operationally used
  qualify row_number() over(
    partition by line, brigade, vehicle_number, position_datetime
    order by request_time_local asc
  ) = 1  -- remove duplicated positions with the same datetime
)
select
  cast(position_datetime as date) as partition_date,
  position_datetime,
  line,
  brigade,
  vehicle_number,
  ST_GEOGPOINT(longitude, latitude) as geom
from data
;

-- backfill
truncate table `warszawski-transport.warsaw_transport.preprocessed_bus_positions`;
insert into `warszawski-transport.warsaw_transport.preprocessed_bus_positions`
with
data as (
  select
    position_datetime,
    line,
    brigade,
    vehicle_number,
    longitude,
    latitude
  from `warszawski-transport.warsaw_transport.raw_bus_positions`
  where 1=1
    and ingestion_date < (select max(ingestion_date) from `warszawski-transport.warsaw_transport.raw_bus_positions`)
    and ingestion_date = cast(position_datetime as date)
    and (request_time_local - position_datetime) < MAKE_INTERVAL(minute => 5)
    and longitude between 20.0 and 22.0  -- remove coordinates outside warsaw metropolitan area
    and latitude between 51.0 and 53.0
    and line not in ('0w', '0z')  -- remove bus lines that are not operationally used
  qualify row_number() over(
    partition by line, brigade, vehicle_number, position_datetime
    order by request_time_local asc
  ) = 1  -- remove duplicated positions with the same datetime
)
select
  cast(position_datetime as date) as partition_date,
  position_datetime,
  line,
  brigade,
  vehicle_number,
  ST_GEOGPOINT(longitude, latitude) as geom
from data
;
