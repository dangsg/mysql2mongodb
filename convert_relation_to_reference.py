import pymongo
from utilities import *
from database_config import mongodb_connection_info
from load_schema import *

def convert_relations_to_references(mongodb_connection_info, schema_file = "schema.json"):
	db_connection = open_connection_mongodb(mongodb_connection_info)
	tables_relations = get_tables_relations(schema_file)
	converting_tables_order = specify_sequence_of_migrating_tables(schema_file)
	edited_table_relations_dict = {}
	original_tables_set = set([tables_relations[key]["source-table"] for key in tables_relations])

	# Edit relations of table dictionary
	for original_table in original_tables_set:
		for key in tables_relations:
			if tables_relations[key]["source-table"] == original_table:
				if original_table not in edited_table_relations_dict.keys():
					edited_table_relations_dict[original_table] = []
				edited_table_relations_dict[original_table] = edited_table_relations_dict[original_table] + [extract_dict(["source-column", "dest-table", "dest-column"])(tables_relations[key])]

	# Convert each relation of each table
	for order in range(int(max(converting_tables_order.keys())) + 1):
		for original_collection_name in converting_tables_order[str(order)]:
			if original_collection_name in original_tables_set:
				for relation_detail in edited_table_relations_dict[original_collection_name]:
					referencing_collection_name = relation_detail["dest-table"]
					original_key = relation_detail["source-column"]
					referencing_key = relation_detail["dest-column"]
					convert_one_relation_to_reference(db_connection, original_collection_name, referencing_collection_name, original_key, referencing_key) 


def convert_one_relation_to_reference(db_connection, original_collection_name, referencing_collection_name, original_key, referencing_key):
	original_collection_connection = db_connection[original_collection_name]
	
	original_documents = original_collection_connection.find()
	new_referenced_key_dict = {}
	for doc in original_documents:
		new_referenced_key_dict[doc[original_key]] = doc["_id"]

	referencing_documents = db_connection[referencing_collection_name]
	for key in new_referenced_key_dict:
		new_reference = {}
		new_reference["$ref"] = original_collection_name
		new_reference["$id"] = new_referenced_key_dict[key]
		referencing_documents.update_many({referencing_key: key}, update={"$set": {referencing_key: new_reference}})

if __name__ == '__main__':
	convert_relations_to_references(mongodb_connection_info)
