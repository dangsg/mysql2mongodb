import pymongo
from utilities import *
from database_config import mongodb_connection_info
from load_schema import *

db_connection = open_connection_mongodb(mongodb_connection_info)
mycol = db_connection["offices"]

# myquery = { "officeCode": "1" }

# mydoc = mycol.find(myquery)
mydoc = mycol.find()

convert_dict = {}

for x in mydoc:
	convert_dict[x["officeCode"]] = x["_id"]
print(convert_dict)

schema_file = "schema.json"

tables_relations = get_tables_relations(schema_file)

converting_tables_order = specify_sequence_of_migrating_tables(schema_file)

# print(tables_relations)
# print(converting_tables_order)

new_dict = {}
for key in tables_relations:
	if tables_relations[key]["source-table"] == 'offices':
		new_dict['offices'] = [extract_dict(["source-column", "dest-table", "dest-column"])(tables_relations[key])]
# print(new_dict)

mycol = db_connection["employees"]
for key in convert_dict:
	mycol.update_many({'officeCode': key}, update={"$set": {'officeCode': convert_dict[key]}})
