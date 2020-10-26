import mysql.connector
import json
import sys
from load_schema import *

def open_connection_mysql(connection_info):
	print(connection_info["host"])
	try:
		db_connection = mysql.connector.connect(
			host=connection_info["host"], 
			user=connection_info["username"], 
			password=connection_info["password"], 
			database=connection_info["database"]
		)
		if db_connection.is_connected():
			db_info = db_connection.get_server_info()
			print("Connected to MySQL Server version ", db_info)

			return db_connection
		else:
			print("Connect fail!")
			return Null
	except Exception as e:
		print("Error while connecting to MySQL", e)

def fetch_table_rows(db_connection, table_name):
	"""Fetch all rows of specific table"""
	db_cursor = db_connection.cursor();
	db_cursor.execute("SELECT * FROM " + table_name)
	fetched_data = db_cursor.fetchall()
	rows = []
	for row in fetched_data:
		rows.append(row)
	db_cursor.close()
	return rows

def fetch_table_columns(db_connection, table_name):
	"""Fetch columns/attribute of table"""
	db_cursor = db_connection.cursor()
	db_cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table_name + "'")
	fetched_data = db_cursor.fetchall()
	columns = []
	for column in fetched_data:
		columns.append(column[0])
	db_cursor.close()
	return columns

def convert_fetched_data_to_json(table_name, rows, columns):
	"""Convert data from table to json"""
	dataset = []
	for row in rows:
		row_dict = {}
		for i in range(len(columns)):
			row_dict[columns[i]] = str(row[i])
		dataset.append(row_dict)
	json_data = {}
	json_data["table"] = table_name
	json_data["data"] = dataset
	return json_data

def write_json_to_file(json_data, filename):
	"""Write json data to file"""
	with open(filename, 'w') as outfile:
		json.dump(json_data, outfile)

def convert_table_to_json(connection_info, table_name, filename):
	"""Write data from a specific table of MySQL to json file"""
	try:
		db_connection = open_connection_mysql(connection_info)
		if db_connection.is_connected():
			rows = fetch_table_rows(db_connection, table_name)
			columns = fetch_table_columns(db_connection, table_name)

			json_data = convert_fetched_data_to_json(table_name, rows, columns)
			write_json_to_file(json_data, filename)
			print("Write data to json file successfully!")
		else:
			print("Connect fail!")
	except Exception as e:
		print("Error while writing to JSON file", e)
	finally:
		if (db_connection.is_connected()):
			db_connection.close()
			print("MySQL connection is closed!")

def convert_all_tables_to_json(connection_info, schema_file):
	tables_name_list = get_tables_name_list(schema_file)

	for table_name in tables_name_list:
		filename = table_name + ".json"
		convert_table_to_json(connection_info, table_name, filename)

if __name__ == '__main__':
	connection_info = {}
	connection_info["host"]="localhost"
	connection_info["username"]="dangsg"
	connection_info["password"]="Db@12345678"
	connection_info["database"]="classicmodels"
	schema_file = "schema.json"
	convert_all_tables_to_json(connection_info, schema_file)

###
# TODO:
# 1. Convert data from MySQL to JSON
# 2. Import data from JSON to MongoDB
###