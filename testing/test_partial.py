import dictdatabase as DDB


def test_subread():
	DDB.config.storage_directory = "./test_db/production_database"
	print(DDB.subread("tasks", key="fM44"))
