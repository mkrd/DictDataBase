import dictdatabase as DDB
import pytest


def test_file_creation(env, use_compression, use_orjson, sort_keys, indent):
	DDB.create("db1", force_overwrite=True)
	db = DDB.read("db1")
	assert db == {}


	DDB.create("tfc2", db=[], force_overwrite=True)
	assert DDB.read("tfc2") == []


	with DDB.session("db1", as_PathDict=True) as (session, d):
		d["a", "b", "c"] = "dee"
		assert d["a", "b", "c"] == "dee"
		session.write()
	assert DDB.read("db1") == {"a": {"b": {"c": "dee"}}}




def test_nested_file_creation(env, use_compression, use_orjson, sort_keys, indent):
	n = DDB.read("blobbles/bla/blub")
	assert n is None
	DDB.create("blobbles/osna/efforts", db={"val": [1, 2]}, force_overwrite=True)
	assert DDB.read("blobbles/osna/efforts") == {"val": [1, 2]}


def test_create_same_file_twice(env, use_compression, use_orjson, sort_keys, indent):
	# Check that creating the same file twice must raise an error
	with pytest.raises(FileExistsError):
		DDB.create("db1", force_overwrite=True)
		DDB.create("db1")

	# Check that creating the same file twice with force_overwrite=True works
	DDB.create("db2", force_overwrite=True)
	DDB.create("db2", force_overwrite=True)
