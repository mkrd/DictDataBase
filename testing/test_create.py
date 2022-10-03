import contextlib
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


def create_same_file_twice():
	# Check that creating the same file twice must raise an error
	with contextlib.suppress(FileExistsError):
		DDB.create("db1")
		DDB.create("db1")
		assert False
	# Check that creating the same file twice with force_overwrite=True works
	DDB.create("db2", force_overwrite=True)
	DDB.create("db2", force_overwrite=True)
