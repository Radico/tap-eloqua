import singer
import backoff
import base64
import time
import requests
import pendulum
import json
import ast

from requests import HTTPError
from tap_kit import BaseClient
from requests import request

LOGGER = singer.get_logger()

# Use base url to get request url
BASE_URL_PATH = 'https://login.eloqua.com/id'
# Path for bulk api
BULK_PATH = '/api/bulk/2.0/'
# Path for rest api
REST_PATH = '/api/REST/2.0/'
# Endpoint for schemas
SCHEMA_ENDPOINT = '/fields'
# Endpoint for data exports
EXPORTS_ENDPOINT = '/exports'
# Endpoint for syncs
SYNC_EXPORT_DATA_ENDPOINT = 'syncs'
# Endpoint for export data
EXPORT_DATA_ENDPOINT = '/data'
# Data endpoints
CONTACTS = 'contacts'
ACTIVITIES = 'activities'

# How long to wait between polling and retries
WAIT_SECS_BETWEEN_STATUS_CHECKS = 20
# How many times to try getting sync status before giving up
MAX_NUM_POLLING_ATTEMPTS = 50
# How many times to retry failed sync before giving up
MAX_RETRY_ATTEMPTS = 20

# Request methods
GET = 'GET'
POST = 'POST'

# Field for event filter
ACTIVITY_TYPE = '{{Activity.Type}}'

# Event stream names
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
    BOUNCES: 'Bounceback'
}

class MaxPollingAttemptsException(Exception):
    pass


class FailedSyncException(Exception):
    pass


class EloquaClient(BaseClient):
    def __init__(self, config):
        """
        Args:
            config: dict
        """
        super().__init__(config)

        self.request_headers = self.build_headers()
        self.base_url = self.build_base_url()
        self.endpoint_name = None

    def build_headers(self):
        """
        These headers should remain the same for all request types
        Returns:
            auth_headers (dict)
        """
        auth_key = self.build_basic_authorization()

        return {
            'Content-Type': 'application/json',
            'Authorization': 'Basic {auth_key}'.format(auth_key=auth_key)
        }

    def build_param(self, key, value, dict={}):
        """
        Generates request parameters
        Args:
            key (str)
            value (str)
            existing params (dict)
        Returns:
            updated params (dict)
        """
        dict[key] = value
        return dict

    def build_basic_authorization(self):
        """
        Encodes the sitename, username, and password to base64
        Returns:
            authorization key (str)
        """
        sitename = self.config.get('sitename')
        username = self.config.get('username')
        password = self.config.get('password')

        key_str = sitename + '\\' + username + ':' + password
        key_bytes = key_str.encode("utf-8")
        auth_key_bytes = base64.b64encode(key_bytes)
        auth_key_str = auth_key_bytes.decode("utf-8")

        return auth_key_str

    def build_base_url(self, method='GET'):
        """
        Need to request the base url from the api
        Args:
            request method (str)
        Returns:
            request url (str)
        """
        path = BASE_URL_PATH
        response = request(method, path, headers=self.request_headers)
        response_json = response.json()
        base_url = response_json.get('urls').get('base')

        return base_url

    def build_request_config(self, url, params=None, run=True):
        """
        Builds request configuration
        Args:
            url (str)
            params (dict)
            run (bool)
        Returns:
            request configuration (dict)
        """
        request_config = {
            'url': url,
            'headers': self.request_headers,
            'params': params,
            'run': run
        }

        return request_config

    def request_stream_schema(self, stream_name):
        """
        Creates request for stream schema and returns schema
        Args:
            stream_name (str)
        Returns:
            field schema (dict)
        """
        data_endpoint = CONTACTS if stream_name == CONTACTS else ACTIVITIES
        request_url = self.base_url + BULK_PATH + data_endpoint + SCHEMA_ENDPOINT
        request_config = self.build_request_config(request_url)
        method = GET

        response = self.make_request(request_config, method=method)
        response_json = response.json()
        schema = response_json.get('items')
        return schema

    def request_bulk_export(self, stream, start_date, end_date, event):
        """
        Creates a data export and returns the export id
        Note the bulk export is unreliable and often
        requires multiple syncs in order to succeed
        Args:
            stream (cls)
            start_date (str)
            end_date (str)
            event (str)
        Returns:
            sync status uri (str)
        """
        self.endpoint_name = ACTIVITIES if event else CONTACTS

        export_uri = self.build_export_definition(stream, start_date, end_date, event)
        sync_status = False
        retries = 0
        while not sync_status:
            LOGGER.info('Attempting sync; %s previous attempt(s) made.' % retries)
            if retries >= MAX_RETRY_ATTEMPTS:
                LOGGER.error('Max number of sync retries made.')
                raise FailedSyncException()

            sync_status_uri = self.synchronize_export_data(export_uri)
            sync_status = self.poll_eloqua_api(sync_status_uri)

            retries = retries + 1
            time.sleep(WAIT_SECS_BETWEEN_STATUS_CHECKS)

        return sync_status_uri

    def build_export_definition(self, stream, start_date, end_date, event):
        """
        Creates a data export and returns an export uri
        Args:
            stream (cls)
            start_date (str)
            end_date (str)
            event (str)
        Returns:
            export uri (str)
        """
        request_body = self.build_export_body(stream, start_date, end_date, event)
        request_url = self.base_url + BULK_PATH + self.endpoint_name + EXPORTS_ENDPOINT
        request_config = self.build_request_config(request_url)
        method = POST

        response = self.make_request(request_config, request_body, method)
        response_json = response.json()
        export_uri = response_json.get('uri')

        return export_uri

    def build_export_body(self, stream, start_date, end_date, event):
        """
        Builds the export body based on the config and stream metadata
        start_date needs to be formatted as 2019-08-06 04:29:15.440
        Args:
            stream (cls)
            start_date (str)
            end_date (str)
            event (str)
        Returns:
            request body (dict)
        """
        stream_name = stream.stream
        name = 'Eloqua {stream_name} stream: {start_date}'.format(
            stream_name=stream_name,
            start_date=start_date
        )

        fields = self.generate_request_fields(stream_name)
        filter_field = fields.get(stream.meta_fields.get('replication_key'))
        filter = "'{filter_field}'>='{start_date}'".format(
            filter_field=filter_field,
            start_date=start_date
        )

        if end_date:
            end_date_filter = "'{filter_field}'<'{end_date}'".format(
                filter_field=filter_field,
                end_date=end_date
            )
            filter = "{start_date_filter} AND {end_date_filter}".format(
                start_date_filter=filter,
                end_date_filter=end_date_filter
            )

        if event:
            event_filter = "'{filter_field}'='{event_type}'".format(
                filter_field=ACTIVITY_TYPE,
                event_type=event
            )
            filter = "{date_filter} AND {event_filter}".format(
                date_filter=filter,
                event_filter=event_filter
            )

        request_body = {
            "name": name,
            "fields": fields,
            "filter": filter
        }

        return request_body

    def generate_request_fields(self, stream_name):
        """
        Generates list of fields to be included in bulk export
        Args:
            stream_name (str)
        Returns:
            request_fields (dict)
        """
        schema = self.request_stream_schema(stream_name)
        request_fields = {}

        # Email engagement streams (everything outside of contacts)
        # will have same schema endpoint but differing schema fields
        # Need to select only fields that are available for each
        # engagement metric
        if stream_name == CONTACTS:
            for field in schema:
                field_name = field.get('internalName').lower()
                request_fields[field_name] = field.get('statement')
        else:
            for field in schema:
                if EVENT_TYPES[stream_name] in field.get('activityTypes'):
                    field_name = field.get('internalName').lower()
                    request_fields[field_name] = field.get('statement')
        return request_fields

    def synchronize_export_data(self, export_uri):
        """
        Creates a sync for the export and returns a status uri
        Args:
            export_uri (str)
        Returns:
            sync_status_uri (str)
        """
        request_body = {
            "syncedInstanceUri": export_uri
        }
        request_url = self.base_url + BULK_PATH + SYNC_EXPORT_DATA_ENDPOINT
        request_config = self.build_request_config(request_url)
        method = POST

        response = self.make_request(request_config, request_body, method)
        response_json = response.json()
        sync_status_uri = response_json.get('uri')

        return sync_status_uri

    def check_sync_status(self, sync_status_uri):
        """
        Takes a sync status uri and retrieves its status
        Args:
            sync_status_uri (str)
        Returns:
            status (str)
        """
        request_url = self.base_url + BULK_PATH + sync_status_uri
        request_config = self.build_request_config(request_url)

        response = self.make_request(request_config)
        response_json = response.json()
        status = response_json.get('status')

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
                    error_msg = 'Errors during sync: {}. ' \
                                'Note that error messages may be ' \
                                'unfortunately vague and refer to ' \
                                'documentation: ' \
                                'https://app.tettra.co/teams/simondata/pages' \
                                '/eloqua-client'.format(errors)
                    LOGGER.error(error_msg)
                    LOGGER.info('Retrying activities export with new sync URI.')
                    return False
                else:
                    error_msg = 'Failure during custom object sync. No ' \
                                'error logs were found from Eloqua.'

                LOGGER.error(error_msg)
                raise FailedSyncException()

        LOGGER.error('Maximum number of polling attemps made.')
        raise MaxPollingAttemptsException()
    
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
        request_config = self.build_request_config(request_url)
        response = self.make_request(request_config)
        response_json = response.json()

        errors = []
        for log_obj in response_json['items']:
            if log_obj['severity'] != 'information':
                errors.append(log_obj['message'])

        return errors

    def fetch_bulk_export_records(self, sync_status_uri, offset, limit, run):
        """
        Once the export data is ready this will retrieve the records from the export
        Args:
            sync_status_uri (str)
            offset (int)
            limit (int)
            run (bool)
        Returns:
            records (dict)
            has_more (bool)
            total_records (int)
        """
        param_payload = self.build_param(key='offset', value=offset)
        self.build_param(key='limit', value=limit, dict=param_payload)
        request_url = self.base_url + BULK_PATH + sync_status_uri + \
            EXPORT_DATA_ENDPOINT
        request_config = self.build_request_config(request_url, param_payload, run)

        response = self.make_request(request_config)
        response_json = response.json()
        records = response_json.get('items')
        has_more = response_json.get('hasMore')
        total_records = response_json.get('totalResults')

        return records, has_more, total_records
