from tap_kit.streams import Stream
import singer


class ContactsStream(Stream):

    def __init__(self, schema_generator):
        super(ContactsStream, self).__init__()
        self.stream = 'contacts'
        self.meta_fields = {
                "key_properties": ['c_emailaddress'],
                "replication_method": 'incremental',
                "replication_key": 'c_datemodified',
                "incremental_search_key": 'c_datemodified',
                "selected_by_default": False
        }
        self.schema = self.generate_schema(schema_generator)

    def generate_schema(self, schema_generator):
        """
        Given the lack of granularity provided by the bulk API, all fields will be proessesd as strings:
        https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAB/Developers/BulkAPI/Tutorials/bulk_data_types.htm
        Args:
            schema_generator (generator)
        Returns:
            schema (dict)
        """
        schema = {
            "properties": {}
        }
        for field in schema_generator:
            schema["properties"][field] = {"type": ["null", "string"]}
        return schema
