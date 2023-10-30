import json

import pytest
from path_dict import pd

import dictdatabase as DDB
from tests.utils import make_complex_nested_random_dict


def test_create(use_compression, use_orjson, indent):
	DDB.at("create").create(force_overwrite=True)
	db = DDB.at("create").read()
	assert db == {}

	with DDB.at("create").session(as_type=pd) as (session, d):
		d["a", "b", "c"] = "😁"
		session.write()
	assert DDB.at("create").read() == {"a": {"b": {"c": "😁"}}}

	with pytest.raises(RuntimeError):
		DDB.at("create", where=lambda k, v: True).create(force_overwrite=True)

	with pytest.raises(RuntimeError):
		DDB.at("create", key="any").create(force_overwrite=True)


def test_create_edge_cases(use_compression, use_orjson, indent):
	cases = [-2, 0.0, "", "x", [], {}, True]

	for i, c in enumerate(cases):
		DDB.at(f"tcec{i}").create(c, force_overwrite=True)
		assert DDB.at(f"tcec{i}").read() == c

	with pytest.raises(TypeError):
		DDB.at("tcec99").create(object(), force_overwrite=True)


def test_nested_file_creation(use_compression, use_orjson, indent):
	n = DDB.at("nested/file/nonexistent").read()
	assert n is None
	db = make_complex_nested_random_dict(12, 6)
	DDB.at("nested/file/creation/test").create(db, force_overwrite=True)
	assert DDB.at("nested/file/creation/test").read() == db


def test_create_same_file_twice(use_compression, use_orjson, indent):
	name = "test_create_same_file_twice"
	# Check that creating the same file twice must raise an error
	with pytest.raises(FileExistsError):
		DDB.at(name).create(force_overwrite=True)
		DDB.at(name).create()
	# Check that creating the same file twice with force_overwrite=True works
	DDB.at(f"{name}2").create(force_overwrite=True)
	DDB.at(f"{name}2").create(force_overwrite=True)
