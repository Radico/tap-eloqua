from tap_kit import TapExecutor
from tap_kit.utils import timestamp_to_iso8601, transform_write_and_count, \
    format_last_updated_for_request
from requests import request

import singer
import pendulum

LOGGER = singer.get_logger()

MAX_RECORDS_RETURNED = 1000

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

        last_updated = format_last_updated_for_request(
            stream.update_and_return_bookmark(), self.replication_key_format
        )
        LOGGER.info("Extracting %s since %s." % (stream, last_updated))

        sync_uri = self.client.request_bulk_export(stream, last_updated)
        offset = 0
        run = True
        latest_record_date = last_updated

        while run:
            records, has_more, total_records = self.client.fetch_bulk_export_records(
                sync_uri, offset, run
            )
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
