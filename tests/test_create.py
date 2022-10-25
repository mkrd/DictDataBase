import dictdatabase as DDB
import pytest
import json

from tests.utils import make_complex_nested_random_dict


def test_create(env, use_compression, use_orjson, sort_keys, indent):
	DDB.create("test_create", force_overwrite=True)
	db = DDB.read("test_create")
	assert db == {}

	with DDB.session("test_create", as_PathDict=True) as (session, d):
		d["a", "b", "c"] = "d"
		session.write()
	assert DDB.read("test_create") == {"a": {"b": {"c": "d"}}}


def test_create_edge_cases(env, use_compression, use_orjson, sort_keys, indent):
	cases = [-2, 0.0, "", "x", [], {}, True]

	for i, c in enumerate(cases):
		DDB.create(f"tcec{i}", db=c, force_overwrite=True)
		assert DDB.read(f"tcec{i}") == c

	with pytest.raises(TypeError):
		DDB.create("tcec11", db=object(), force_overwrite=True)


def test_nested_file_creation(env, use_compression, use_orjson, sort_keys, indent):
	n = DDB.read("nested/file/nonexistent")
	assert n is None
	db = make_complex_nested_random_dict(12, 6)
	DDB.create("nested/file/creation/test", db=db, force_overwrite=True)
	assert DDB.read("nested/file/creation/test") == db


def test_create_same_file_twice(env, use_compression, use_orjson, sort_keys, indent):
	name = "test_create_same_file_twice"
	# Check that creating the same file twice must raise an error
	with pytest.raises(FileExistsError):
		DDB.create(name, force_overwrite=True)
		DDB.create(name)
	# Check that creating the same file twice with force_overwrite=True works
	DDB.create(f"{name}2", force_overwrite=True)
	DDB.create(f"{name}2", force_overwrite=True)
