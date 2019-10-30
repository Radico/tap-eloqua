from tap_kit.streams import Stream
import singer


class SendsStream(Stream):

    def __init__(self, schema_generator):
        super(SendsStream, self).__init__()
        self.stream = 'sends'
        self.meta_fields = dict(
            key_properties=['id'],
            replication_method='incremental',
            replication_key='activitydate',
            incremental_search_key='activitydate',
            selected_by_default=False
        )
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
