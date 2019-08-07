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

        self.replication_key_format = 'timestamp'

    def call_incremental_stream(self, stream):
        """Method to call incrementally synced streams"""
        """TODO: only for bulk api, update for rest api too"""

        last_updated = format_last_updated_for_request(
            stream.update_and_return_bookmark(), self.replication_key_format
        )
        LOGGER.info("Extracting %s since %s." % (stream, last_updated))

        sync_uri = self.client.request_bulk_export(stream, last_updated)
        offset = 0
        has_more = True

        while has_more:
            records, has_more = self.client.fetch_bulk_export_records(sync_uri, offset)
            transform_write_and_count(stream, records)
            offset = offset + MAX_RECORDS_RETURNED

