from tap_kit import TapExecutor
from tap_kit.utils import timestamp_to_iso8601, transform_write_and_count, \
    format_last_updated_for_request
from requests import request
from datetime import datetime

import singer
import pendulum

LOGGER = singer.get_logger()

# Extract limit per request
MAX_RECORDS_RETURNED = 50000

# Export dataset size limit
EXPORT_LIMIT = 5000000

# Stream names
ACTIVITIES = 'activities'
CONTACTS = 'contacts'

# Event type filters
EVENT_TYPES = {
    ACTIVITIES: [
        "EmailSend",
        "EmailOpen",
        "EmailClickthrough",
        "Subscribe",
        "Unsubscribe",
        "Bounceback"
    ],
    CONTACTS: [
        None
    ]
}

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

    def call_incremental_stream(self, stream):
        """Method to call incrementally synced streams"""
        """TODO: only for bulk api, update for rest api too"""
        stream_name = stream.stream
        last_updated = format_last_updated_for_request(
            stream.update_and_return_bookmark(), self.replication_key_format
        )
        LOGGER.info("Extracting %s since %s." % (stream_name, last_updated))
        latest_record_date = last_updated
        start_date = pendulum.parse(last_updated)
        end_date = pendulum.now()

        events = EVENT_TYPES.get(stream_name)
        for event in events:
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

                sync_uri = self.client.request_bulk_export(stream, request_start_str, request_end_str, event)
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
