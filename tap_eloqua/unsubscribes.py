from tap_kit.streams import Stream
import singer


class UnsubscribesStream(Stream):

    stream = 'unsubscribes'

    meta_fields = dict(
        key_properties=['id'],
        replication_method='incremental',
        replication_key='activity_date',
        incremental_search_key='activity_date',
        selected_by_default=False
    )

    schema = {
        "properties": {
            'campaign_id': {
                'type': ['null', 'string']
            },
            'contact_id': {
                'type': ['null', 'string']
            },
            'campaign_name': {
                'type': ['null', 'string']
            },
            'email_address': {
                'type': ['null', 'string']
            },
            'activity_id': {
                'type': ['null', 'string']
            },
            'asset_id': {
                'type': ['null', 'string']
            },
            'external_campaign_id': {
                'type': ['null', 'string']
            },
            'asset_name': {
                'type': ['null', 'string']
            },
            'asset_type': {
                'type': ['null', 'string']
            },
            'activity_date': {
                'type': ['null', 'string']
            },
            'external_id': {
                'type': ['null', 'string']
            },
            'activity_type': {
                'type': ['null', 'string']
            },
            'email_recipient_id': {
                'type': ['null', 'string']
            }
        }
    }

