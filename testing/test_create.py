
import dictdatabase as DDB


def file_creation():
	n = DDB.read("Non_existent")
	assert n is None

	DDB.create("db1")
	db = DDB.read("db1")
	assert db == {}

	with DDB.session("db1", as_PathDict=True) as (session, d):
		d["a", "b", "c"] = "dee"
		assert d["a", "b", "c"] == "dee"
		session.write()
	assert DDB.read("db1") == {"a": {"b": {"c": "dee"}}}





def nested_file_creation():
	n = DDB.read("blobbles/bla/blub")
	assert n is None
	DDB.create("blobbles/osna/efforts", db={"val": [1, 2]})
	assert DDB.read("blobbles/osna/efforts") == {"val": [1, 2]}
