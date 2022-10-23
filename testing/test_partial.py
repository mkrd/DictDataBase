import dictdatabase as DDB


def read_non_existent_json():
	DDB.config.storage_directory = "./test_db/production_database"
	print(DDB.subread("tasks", key="fM44"))
