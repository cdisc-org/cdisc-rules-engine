import os
import json
import jsonschema


from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.adam_variable_reader import AdamVariableReader
from cdisc_rules_engine.services.datasetjson_metadata_reader import DatasetJSONMetadataReader
from cdisc_rules_engine.services.datasetndjson_metadata_reader import DatasetNDJSONMetadataReader
from cdisc_rules_engine.services.data_readers.json_reader import DatasetJSONReader
from cdisc_rules_engine.services.data_readers.ndjson_reader import DatasetNDJSONReader


# obj = DatasetJSONMetadataReader("C:\\Users\\U062293\\OneDrive - UCB\\Documents\\GitHub\\Examples\\adam\\ADAE.json", "ADAE.json")

# obj.read()

obj = DatasetNDJSONReader()

df = obj.from_file("C:\\Users\\U062293\\OneDrive - UCB\\Documents\\GitHub\\Examples\\sdtm_ndjson\\AE.ndjson")

print(df._data)


# metadatandjson, datandjson = obj.read_json_file("C:\\Users\\U062293\\OneDrive - UCB\\Documents\\GitHub\\Examples\\sdtm_ndjson\\AE.ndjson")

# print(datandjson)


