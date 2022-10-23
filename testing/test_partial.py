import dictdatabase as DDB
import json


def test_subread():
	DDB.config.storage_directory = "./test_db/production_database"

	j = {
		"a": "Hello\{\}",
		"b": [0, 1],
		"c": {"d": "e"},
	}

	DDB.create("test_subread", db=j, force_overwrite=True)


	try:
		DDB.subread("test_subread", key="a") == "Hello{\}"
	except json.decoder.JSONDecodeError:
		pass

	try:
		DDB.subread("test_subread", key="f")
	except KeyError:
		pass

	assert DDB.subread("test_subread", key="b") == [0, 1]
	assert DDB.subread("test_subread", key="c") == {"d": "e"}


def test_subwrite():
	j = {
		"b": {"0": 1},
		"c": {"d": "e"},
	}
	DDB.config.storage_directory = "./test_db/production_database"

	DDB.config.use_orjson = False
	DDB.config.indent_with = "  "
	DDB.config.sort_keys = False


	DDB.create("test_subwrite", db=j, force_overwrite=True)
	with DDB.subsession("test_subwrite", key="c", as_PathDict=True) as (session, task):
		task["f"] = lambda x: (x or 0) + 1
		session.write()

	with DDB.subsession("test_subwrite", key="b", as_PathDict=True) as (session, task):
		# task["f"] = lambda x: (x or 0) + 1
		session.write()
