from .contacts import ContactsStream
from .bounces import BouncesStream
from .clicks import ClicksStream
from .opens import OpensStream
from .sends import SendsStream
from .subscribes import SubscribesStream
from .unsubscribes import UnsubscribesStream
from tap_kit import TapExecutor
from tap_kit.utils import timestamp_to_iso8601, transform_write_and_count, \
    format_last_updated_for_request
from requests import request
from datetime import datetime

import json
import pendulum
import singer
import sys

LOGGER = singer.get_logger()

# Extract limit per request
MAX_RECORDS_RETURNED = 50000

# Export dataset size limit
EXPORT_LIMIT = 5000000

# Stream names
CONTACTS = 'contacts'
SENDS = 'sends'
OPENS = 'opens'
CLICKS = 'clicks'
SUBSCRIBES = 'subscribes'
UNSUBSCRIBES = 'unsubscribes'
BOUNCES = 'bounces'

# Event type filters
EVENT_TYPES = {
    SENDS:'EmailSend',
    OPENS:'EmailOpen',
    CLICKS:'EmailClickthrough',
    SUBSCRIBES: 'Subscribe',
    UNSUBSCRIBES: 'Unsubscribe',
    BOUNCES: 'Bounceback',
    CONTACTS: None
}

# Streams with dynamic schemas
DYNAMIC_SCHEMAS = [
    (CONTACTS, ContactsStream),
    (SENDS, SendsStream),
    (OPENS, OpensStream),
    (CLICKS, ClicksStream),
    (SUBSCRIBES, SubscribesStream),
    (UNSUBSCRIBES, UnsubscribesStream),
    (BOUNCES, BouncesStream)
]

class EloquaExecutor(TapExecutor):

    def __init__(self, streams, args, client):
        """
        Args:
            streams: arr[Stream]
            args: dict
            client: BaseClient
        """
        super().__init__(streams, args, client)

        self.replication_key_format = 'datetime_string'

    def discover(self):
        """
        Replace standard tap-kit discover in order to work with
        dynamic schemas.
        Returns:
            catalog (json)
        """
        catalog = []

        for stream in DYNAMIC_SCHEMAS:
            stream_name = stream[0]
            stream_class = stream[1]
            '''catalog.append(
                stream_class(
                    self.schema_generator(stream_name)
                ).generate_catalog()
            )'''
            contacts = stream_class(self.schema_generator(stream_name))
            catalog.append(contacts.generate_catalog())

        return json.dump({'streams': catalog}, sys.stdout, indent=4)

    def schema_generator(self, stream_name):
        """
        The fields associated with each Eloqua stream varies from
        organization to organization. Need to dynamically generate
        if all fields are going to be selected for extraction.
        Args:
            stream_name (str)
        Returns:
            schema (generator)
        """
        schema = self.client.request_stream_schema(stream_name)

        # Email engagement streams (everything outside of contacts)
        # will have same schema endpoint but differing schema fields
        # Need to select only fields that are available for each
        # engagement metric
        if stream_name == CONTACTS:
            for field in schema:
                yield field.get('internalName').lower()
        else:
            for field in schema:
                if EVENT_TYPES[stream_name] in field.get('activityTypes'):
                    yield field.get('internalName').lower()

    def call_incremental_stream(self, stream):
        """
        Method to call incrementally synced streams
        TODO: only for bulk api, update for rest api too
        Args:
            stream (cls)
        Returns:
            last_record_date (dttime)
        """
        stream_name = stream.stream
        event_name = EVENT_TYPES.get(stream_name)
        last_updated = format_last_updated_for_request(
            stream.update_and_return_bookmark(), self.replication_key_format
        )
        LOGGER.info("Extracting %s since %s." % (stream_name, last_updated))
        latest_record_date = last_updated
        start_date = pendulum.parse(last_updated)
        end_date = pendulum.now()

        requests = [(start_date, end_date)]

        while requests:
            request_filters = requests.pop(0)
            request_start = request_filters[0]
            request_end = request_filters[1]
            request_start_str = request_start.to_datetime_string()
            request_end_str = request_end.to_datetime_string()
            LOGGER.info('Requesting export from %s to %s.' % (
                request_start_str, request_end_str
            ))

            sync_uri = self.client.request_bulk_export(stream, request_start_str, request_end_str, event_name)
            offset = 0
            run = True

            while run:
                records, has_more, total_records = self.client.fetch_bulk_export_records(
                    sync_uri, offset, MAX_RECORDS_RETURNED, run
                )
                if total_records >= EXPORT_LIMIT:
                    LOGGER.info('Export exceeds 5M record limit. Splitting into multiple requests.')
                    new_end_date = request_start + ((request_end - request_start) / 2)
                    requests.append((request_start, new_end_date))
                    requests.append((new_end_date, request_end))
                    run = False

                elif total_records == 0:
                    LOGGER.info('No records found between %s and %s.' % (request_start, request_end))
                    run = False

                else:
                    transform_write_and_count(stream, records)

                    latest_record_batch = self.get_latest_for_next_call(
                        records=records,
                        replication_key=stream.meta_fields.get('replication_key'),
                        last_updated=last_updated
                    )
                    if latest_record_batch > latest_record_date:
                        latest_record_date = latest_record_batch
                    
                    if not has_more:
                        run = False
                        LOGGER.info('Completed fetching records. Fetched %s records.' % total_records)

                    else:
                        offset = offset + MAX_RECORDS_RETURNED
                        LOGGER.info('Fetched %s of %s records. Fetching next set of records.' % (
                            offset, total_records
                        ))

        return latest_record_date
