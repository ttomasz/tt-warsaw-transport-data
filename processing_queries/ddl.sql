create or replace view warsaw_transport.last_positions_from_raw_v as
select
  ST_GEOGPOINT(longitude, latitude) as geom,
  position_datetime,
  line,
  brigade,
  vehicle_number,
  row_number() over(partition by line, brigade, vehicle_number order by position_datetime desc) position_number
from warsaw_transport.raw_bus_positions
where
  ingestion_date = CURRENT_DATE('Europe/Warsaw') --(select max(ingestion_date) from warsaw_transport.raw_bus_positions)
  and
  (request_time_local - position_datetime) < MAKE_INTERVAL(minute => 5)
qualify
  position_number <= 5
  and
  row_number() over(partition by line, brigade, vehicle_number, position_datetime) = 1 
order by line, brigade, position_number
;

create table warsaw_transport.preprocessed_bus_positions (
    partition_date date not null,
    position_datetime datetime,
    line string,
    brigade string,
    vehicle_number string,
    geom geography
)
PARTITION BY partition_date
CLUSTER BY position_datetime
OPTIONS (
    description = 'Positions of Buses in Warsaw. Preprocessed to eliminate wrong/old/duplicated data.',
    partition_expiration_days = NULL
);

create table warsaw_transport.processed_bus_positions (
    partition_date date not null,
    position_datetime datetime not null,
    line string not null,
    brigade string not null,
    vehicle_number string not null,
    geom geography not null,
    prev_position_datetime datetime,
    prev_geom geography,
    seconds_between_measurements float64,
    distance_m float64,
    speed_kmh float64
)
PARTITION BY partition_date
CLUSTER BY position_datetime
OPTIONS (
    description = 'Positions of Buses in Warsaw. Removed positions from specific areas. Calculated speed.',
    partition_expiration_days = NULL
);

create or replace view warsaw_transport.enriched_bus_positions as
  select
    position_datetime,
    partition_date,
    cast(position_datetime as time) as position_time,
    cast(DATETIME_BUCKET(position_datetime, INTERVAL 5 minute) as time) as time_bucket_5min,
    cast(DATETIME_BUCKET(position_datetime, INTERVAL 15 minute) as time) as time_bucket_15min,
    cast(DATETIME_BUCKET(position_datetime, INTERVAL 1 hour) as time) as time_bucket_1h,
    line,
    brigade,
    vehicle_number,
    seconds_between_measurements,
    distance_m,
    speed_kmh,
    geom,
    prev_position_datetime,
    prev_geom,
    `warszawski-transport`.carto.H3_FROMGEOGPOINT(geom, 10) as h3_z10,
    `warszawski-transport`.carto.H3_FROMGEOGPOINT(geom, 11) as h3_z11,
    `warszawski-transport`.carto.H3_FROMGEOGPOINT(geom, 12) as h3_z12
  from warsaw_transport.processed_bus_positions
;
