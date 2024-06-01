from copy import copy
import logging
from pathlib import Path
import sys
from dataclasses import dataclass
from datetime import timedelta, datetime, date
from os import getenv
from time import sleep
from typing import Sequence

import requests
from dotenv import load_dotenv
import pytz
from google.cloud.bigquery.table import TableReference
from google.cloud.bigquery_storage_v1.services.big_query_write.client import BigQueryWriteClient
from google.cloud.bigquery_storage_v1.types import (
    AppendRowsRequest,
    ProtoRows,
    ProtoSchema,
)
from google.protobuf.descriptor_pb2 import DescriptorProto
from google.protobuf.message import Message

from BusPosition_pb2 import BusPosition as BusPositionMessage


LOCAL_TIMEZONE = pytz.timezone('Europe/Warsaw')
ENDPOINT_URL = 'https://api.um.warszawa.pl/api/action/busestrams_get/'
RESOURCE_ID = 'f2e5503e-927d-4ad3-9500-4ab9e55deb59'
BUS_TYPE = 1
TRAM_TYPE = 2
bq_table_name = 'warszawski-transport.warsaw_transport.raw_bus_positions'

# fieldnames = ['Lines', 'Lon', 'VehicleNumber', 'Time', 'Lat', 'Brigade']
sleep_time = timedelta(seconds=10.0)


@dataclass(frozen=True)
class BusPosition:
    request_time_local: datetime
    ingestion_date: date
    position_datetime: str
    line: str
    brigade: str
    vehicle_number: str
    latitude: float
    longitude: float

    def as_pbf_message(self) -> BusPositionMessage:
        return BusPositionMessage(
            request_time_local=self.request_time_local.isoformat(),
            ingestion_date=self.ingestion_date.toordinal() - date(1970, 1, 1).toordinal(),
            position_datetime=self.position_datetime,
            line=self.line,
            brigade=self.brigade,
            vehicle_number=self.vehicle_number,
            latitude=self.latitude,
            longitude=self.longitude,
        )


def get_data() -> list[BusPosition]:
    logging.info('Sending request.')
    request_time = datetime.now(tz=LOCAL_TIMEZONE)
    response = requests.post(
        url=ENDPOINT_URL,
        params=dict(
            resource_id=RESOURCE_ID,
            apikey=getenv('warsaw_api_key'),
            type=BUS_TYPE,
        ),
        timeout=timedelta(seconds=10).total_seconds(),
    )
    response.raise_for_status()
    positions = response.json()['result']
    if type(positions) != list:
        raise ValueError(f'Bad response. Result: {positions}')
    logging.info(f'API returned: {len(positions)} positions.')
    data = [
        BusPosition(
            request_time_local=request_time.replace(tzinfo=None),
            ingestion_date=request_time.date(),
            position_datetime=position['Time'],
            line=position['Lines'],
            brigade=position['Brigade'],
            vehicle_number=position['VehicleNumber'],
            latitude=position['Lat'],
            longitude=position['Lon'],
        )
        for position in positions
        if all([
            position.get('Time'),
            position.get('Lines'),
            position.get('Brigade'),
            position.get('VehicleNumber'),
            position.get('Lat'),
            position.get('Lon'),
        ])
    ]
    logging.info(f'Parsed: {len(data)} positions.')
    return data


def stream_to_bq(table: str, messages: Sequence[Message], client: BigQueryWriteClient) -> None:
    """
    Use Storage Write API to stream rows into the BQ table.

    Args:
        table:
            A table ID in standard SQL format. It must include
            project ID, dataset ID, and table ID, each separated by ``.``.
        messages:
            List of gRPC messages that would be pushed to the BQ table.
    """

    table_ref = TableReference.from_string(table)
    write_stream_name = f'{table_ref.path}/streams/_default'.lstrip('/')
    logging.info(f'Writing to BigQuery to stream: {write_stream_name}')

    try:
        results = client.append_rows(
            # our messages from a single run are well below write api limit https://cloud.google.com/bigquery/quotas#write-api-limits
            # so we will send all of them in a single request
            iter([_build_append_rows_request(messages, write_stream_name)])
        )
    except:
        logging.exception('Unexpected exception while streaming messages.')
        results = []

    for result in results:
        if 'error' in result:
            logging.error(f'BigQuery returned error in results: {result.error}. {list(result.row_errors)}')
    
    logging.info('Finished writing to BigQuery.')


def _build_append_rows_request(messages: Sequence[Message], stream_name: str) -> AppendRowsRequest:
    """
    Create AppendRowsRequest() with messages included.
    For the first request we need to include stream name and protobuf schema
    of the message. Remaining messages might skip it.
    """
    assert messages
    rows = ProtoRows(serialized_rows=[message.SerializeToString() for message in messages])
    request = AppendRowsRequest()

    first_message = messages[0]
    proto_descriptor = DescriptorProto()
    first_message.DESCRIPTOR.CopyToProto(proto_descriptor)

    request.write_stream = stream_name
    request.proto_rows = AppendRowsRequest.ProtoData(
        writer_schema=ProtoSchema(proto_descriptor=proto_descriptor),
        rows=rows,
    )

    return request


if __name__ == '__main__':
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=logging_format, level=logging.INFO, stream=sys.stdout)
    logging.info('Starting script.')
    load_dotenv()

    credentials_file = getenv('gcp_credentials_file_path', default='')
    credentials_file_path = Path(credentials_file)
    if not credentials_file or not credentials_file_path.is_file():
        raise Exception('Could not find GCP credentials file.')

    while True:
        try:
            bq_client = BigQueryWriteClient.from_service_account_file(filename=credentials_file)
            data = get_data()
        except Exception as e:
            logging.exception('Something went wrong during request.')
            logging.info(f'Going to sleep for {sleep_time.total_seconds()}s')
            sleep(sleep_time.total_seconds())
            continue
        if len(data) > 0:
            messages = [position.as_pbf_message() for position in data]
            stream_to_bq(table=bq_table_name, messages=messages, client=bq_client)
            bq_client.transport.close()
        logging.info(f'Going to sleep for {sleep_time.total_seconds()}s')
        sleep(sleep_time.total_seconds())
    logging.info('Loop broken. Exiting.')
