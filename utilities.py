from pymongo import MongoClient  

def extract_dict(selected_keys):
	def extract_dict(input_dict):
		output_dict = {}
		for key in selected_keys:
			output_dict[str(key)] = input_dict[str(key)]
		return output_dict
	return extract_dict

def open_connection_mongodb(connection_info):
	try:
		# Making connection 
		mongo_client = MongoClient(connection_info["connection_string"])  
		# Select database  
		db_connection = mongo_client[connection_info["database_name"]] 		
		return db_connection
	except Exception as e:
		print("Error while connecting to MongoDB", e)
		raise e