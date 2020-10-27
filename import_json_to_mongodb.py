import json 
import sys
from load_schema import *
from utilities import *
  
def import_json_to_mongodb(db_connection, collection_name, json_filename):
	try:		   
		# Created or switched to collection  
		Collection = db_connection[collection_name] 
		  
		# Loading or Opening the json file 
		with open(json_filename) as file: 
		    file_data = json.load(file) 
		    table_data = file_data["data"]

		# Inserting the loaded data in the Collection 
		# if JSON contains data more than one entry 
		# insert_many is used else inser_one is used 
		if isinstance(table_data, list): 
		    Collection.insert_many(table_data)   
		else: 
		    Collection.insert_one(table_data) 
	except Exception as e:
		print("Error while writing to MongoDB", e)
		raise e

def import_all_json_to_mongodb(db_connection, schema_file):
	tables_name_list = get_tables_name_list(schema_file)

	for table_name in tables_name_list:
		collection_name = table_name
		json_filename = collection_name + ".json"
		import_json_to_mongodb(db_connection, collection_name, json_filename)


if __name__ == '__main__':
	connection_info = {}
	connection_info["connection_string"] = "mongodb://localhost:27017/"
	connection_info["database_name"] = "data-conv-1"
	db_connection = open_connection_mongodb(connection_info) 
	schema_file = "schema.json"
	import_all_json_to_mongodb(db_connection, schema_file)