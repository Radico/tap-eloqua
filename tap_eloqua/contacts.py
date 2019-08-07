from tap_kit.streams import Stream
import singer


class ContactsStream(Stream):

    stream = 'contacts'

    meta_fields = dict(
        key_properties=['id'],
        replication_method='incremental',
        replication_key='modified_date',
        incremental_search_key='updated_after',
        selected_by_default=False
    )

    schema = {
        "contact_id": {
            "type": ["null", "string"]
        },
        "email_address": {
            "type": ["null", "string"]
        },
        "eloqua_contact_id": {
            "type": ["null", "string"]
        },
        "organization_user_id": {
            "type": ["null", "string"]
        },
        "modified_date": {
            "type": ["null", "string"]
        },
        "modified_date_crm": {
            "type": ["null", "string"]
        }
    }