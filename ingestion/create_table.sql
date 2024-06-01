create table warsaw_transport.raw_bus_positions (
    request_time_local datetime not null,
    ingestion_date date not null,
    position_datetime datetime,
    line string,
    brigade string,
    vehicle_number string,
    latitude float64,
    longitude float64
)
PARTITION BY ingestion_date
CLUSTER BY request_time_local
OPTIONS (
    description = 'Positions of Buses in Warsaw. Raw data from the API. Ingestion date is date of request_time_local.'
);
