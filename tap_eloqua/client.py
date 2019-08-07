import singer
import backoff
import base64
import time
import requests

from requests import HTTPError
from tap_kit import BaseClient

LOGGER = singer.get_logger()

# Use base url to get request url
BASE_URL_PATH = 'https://login.eloqua.com/id'
# Path for bulk api
BULK_PATH = '/api/bulk/2.0/'
# Path for rest api
REST_PATH = '/api/REST/2.0/'
# Endpoint for data exports
EXPORTS_ENDPOINT = '/exports'
# Endpoint for syncs
SYNC_EXPORT_DATA_ENDPOINT = 'syncs'
# Endpoint for export data
EXPORT_DATA_ENDPOINT = '/data'
# Import batch size
MAX_BULK_REQUEST = 50000
# How long to wait before polling the status API again
WAIT_SECS_BETWEEN_STATUS_CHECKS = 5
# How many milliseconds to wait between consecutive requests. Need a small
# delay to stay below limits for Eloqua API.
WAIT_BETWEEN_REQUESTS_MS = 150
# How many times to try getting sync status before giving up
MAX_NUM_POLLING_ATTEMPTS = 10


class EloquaClient(BaseClient):
    def __init__(self, config):
        """
        Args:
            config: dict
        """
        super().__init__(config)

        self.request_headers = self.build_headers()
        self.base_url = self.build_base_url()

    @staticmethod
    def requests_method(method, request_config, body):
        if 'Content-Type' not in request_config['headers']:
            request_config['headers']['Content-Type'] = 'application/json'

        return requests.request(
            method,
            request_config['url'],
            headers=request_config['headers'],
            json=body
        )

    def build_headers(self):
        """These headers should remain the same for all request types"""
        auth_key = self.build_basic_authorization()

        return {
            'Content-Type': 'application/json',
            'Authorization': 'Basic {auth_key}'.format(auth_key=auth_key)
        }

    def build_basic_authorization(self):
        """Encodes the sitename, username, and password to base64"""
        sitename = self.config.get('sitename')
        username = self.config.get('username')
        password = self.config.get('password')

        key_before_encoding = sitename + '\\' + username + ':' + password
        auth_key = base64.b64encode(key_before_encoding)

        return auth_key

    def build_base_url(self):
        """Need to request the base url from the api"""
        path = BASE_URL_PATH
        base_url = self.make_request(path)
        return base_url

    def request_bulk_export(self, stream, start_date):
        """Creates a data export and returns the export id"""
        export_uri = self.build_export_definition(stream, start_date)
        sync_status_uri = self.synchronize_export_data(export_uri)
        self.poll_eloqua_api(sync_status_uri)
        return sync_status_uri

    def build_export_definition(self, stream, start_date):
        """Creates a data export and returns an export uri"""
        request_body = self.build_export_body(stream, start_date)
        request_url = self.base_url + BULK_PATH + stream.name + EXPORTS_ENDPOINT
        request_config = {
            'url': request_url,
            'headers': self.request_headers,
            'run': True
        }
        method = 'POST'

        response = self.make_request(request_config, request_body, method)
        export_uri = response.content.uri
        return export_uri

    def build_export_body(self, stream, start_date):
        """Builds the export body based on the stream metadata"""
        """start_date needs to be formatted as 2019-08-06 04:29:15.440"""
        name = 'Eloqua {stream_name} stream: {start_date}'.format(
            stream_name=stream.name,
            start_date=start_date
        )
        fields = self.config.export_fields

        request_body = {
            "name": name,
            "fields": fields
        }

        if stream.replication_method == 'incremental':
            """TODO: replace with actual filter field"""
            filter = "'{filter_field}'>='{start_date}'".format(
                filter_field=stream.filter_field,
                start_date=start_date
            )
            request_body["filter"] = filter

        return request_body

    def synchronize_export_data(self, export_uri):
        """Creates a sync for the export and returns a status uri"""
        request_body = {
            "syncedInstanceUri": export_uri
        }
        request_url = self.base_url + BULK_PATH + SYNC_EXPORT_DATA_ENDPOINT
        request_config = {
            'url': request_url,
            'headers': self.request_headers,
            'run': True
        }
        method = 'POST'

        response = self.make_request(request_config, request_body, method)
        sync_status_uri = response.content.uri

        return sync_status_uri

    def check_sync_status(self, sync_status_uri):
        """Takes a sync status uri and retrieves its status"""
        request_url = self.base_url + BULK_PATH + sync_status_uri
        request_config = {
            'url': request_url,
            'headers': self.request_headers,
            'run': True
        }

        response = self.make_request(request_config)
        status = response.content.status
        return status

    def poll_eloqua_api(self, sync_status_uri):
        """
        Try fetching sync status several times with delays.
        Args:
            sync_status_uri(str): URI to check sync status

        Raises:
            GenericChannelException: If sync status is not 'success' or can't
               fetch status.
        """
        """TODO: better way to raise error for failed sync"""
        num_polling_attempts = 0
        while num_polling_attempts < MAX_NUM_POLLING_ATTEMPTS:
            LOGGER.info(
                'Polling Eloqua API. Tries: {}'.format(num_polling_attempts)
            )

            num_polling_attempts += 1
            time.sleep(WAIT_SECS_BETWEEN_STATUS_CHECKS)
            status = self.check_sync_status(sync_status_uri)
            if status == 'success':
                LOGGER.info('Eloqua sync successfully completed')
                return True
            elif status == 'active':
                LOGGER.info('Eloqua sync not completed yet - try %d out of %d',
                            num_polling_attempts,
                            MAX_NUM_POLLING_ATTEMPTS)

            # 'warning' and 'error' are both considered errors for eloqua
            else:
                LOGGER.error('Eloqua export sync failed.')
                sync_logs_uri = sync_status_uri + '/logs'
                errors = self.fetch_sync_logs(sync_logs_uri)
                if errors:
                    error_msg = 'Errors during custom object sync: {}. ' \
                                'Note that error messages may be ' \
                                'unfortunately vague and refer to ' \
                                'documentation: ' \
                                'https://app.tettra.co/teams/simondata/pages' \
                                '/eloqua-client'.format(errors)
                else:
                    error_msg = 'Failure during custom object sync. No ' \
                                'error logs were found from Eloqua.'

                LOGGER.error(error_msg)
    
    def fetch_sync_logs(self, sync_logs_uri):
        """
        Sends a Get request in the event of a sync failure to get the sync
        logs from Eloqua. Parses out possible error messages based on log
        severity and returns them.
        Args:
            sync_logs_uri (unicode)
        Returns:
            errors (list)
        """
        request_url = self.base_url + BULK_PATH + sync_logs_uri
        request_config = {
            'url': request_url,
            'headers': self.request_headers,
            'run': True
        }
        sync_logs_response = self.make_request(
            request_config
        )

        errors = []
        for log_obj in sync_logs_response['items']:
            if log_obj['severity'] != 'information':
                errors.append(log_obj['message'])

        return errors

    def fetch_bulk_export_records(self, sync_status_uri, offset):
        """Once the export data is ready this will retrieve the records from the export"""
        offset_param = '?offset={offset}'.format(offset=offset)
        request_url = self.base_url + BULK_PATH + sync_status_uri + \
            EXPORT_DATA_ENDPOINT + offset_param
        request_config = {
            'url': request_url,
            'headers': self.request_headers,
            'run': True
        }

        response = self.make_request(request_config)
        records = response.content.items
        has_more = True if response.content.hasMore == 'true' else False
        return records, has_more
