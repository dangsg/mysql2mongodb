import os

def generate_mysql_schema(host, port, database_name, username, password, info_level, schemacrawler_command):
	command_create_intermediate_dir = f"mkdir -p ./intermediate_data/{database_name}"
	os.system(command_create_intermediate_dir)

	command = f"schemacrawler.sh \
	--server=mysql \
	--host={host} \
	--port={port} \
	--database={database_name} \
	--schemas={database_name} \
	--user={username} \
	--password={password} \
	--info-level={info_level} \
	--command={schemacrawler_command}\
	--output-file=./intermediate_data/{database_name}/schema.json"
	os.system(command)

	print(f"Generate MySQL database {database_name} successfully!")

if __name__ == '__main__':
	host = 'localhost'
	port = '3306'
	database_name = 'sakila'
	username = 'dangsg'
	password = 'Db@12345678'
	info_level = 'standard'
	schemacrawler_command = 'serialize'
	generate_mysql_schema(host, port, database_name, username, password, info_level, schemacrawler_command)