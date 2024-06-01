from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class BusPosition(_message.Message):
    __slots__ = ("request_time_local", "ingestion_date", "position_datetime", "line", "brigade", "vehicle_number", "latitude", "longitude")
    REQUEST_TIME_LOCAL_FIELD_NUMBER: _ClassVar[int]
    INGESTION_DATE_FIELD_NUMBER: _ClassVar[int]
    POSITION_DATETIME_FIELD_NUMBER: _ClassVar[int]
    LINE_FIELD_NUMBER: _ClassVar[int]
    BRIGADE_FIELD_NUMBER: _ClassVar[int]
    VEHICLE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    request_time_local: str
    ingestion_date: int
    position_datetime: str
    line: str
    brigade: str
    vehicle_number: str
    latitude: float
    longitude: float
    def __init__(self, request_time_local: _Optional[str] = ..., ingestion_date: _Optional[int] = ..., position_datetime: _Optional[str] = ..., line: _Optional[str] = ..., brigade: _Optional[str] = ..., vehicle_number: _Optional[str] = ..., latitude: _Optional[float] = ..., longitude: _Optional[float] = ...) -> None: ...
