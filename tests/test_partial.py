import dictdatabase as DDB
import json
import pytest


def test_subread(env, use_compression, use_orjson, sort_keys, indent):
	j = {
		"a": "Hello{}",
		"b": [0, 1],
		"c": {"d": "e"},
	}

	DDB.at("test_subread").create(j, force_overwrite=True)
	with pytest.raises(json.decoder.JSONDecodeError):
		DDB.subread("test_subread", key="a") == "Hello}{"

	with pytest.raises(KeyError):
		DDB.subread("test_subread", key="f")

	assert DDB.subread("test_subread", key="b") == [0, 1]
	assert DDB.subread("test_subread", key="c") == {"d": "e"}

	j2 = {"a": {"b": "c"}, "b": {"d": "e"}}
	DDB.at("test_subread2").create(j2, force_overwrite=True)
	assert DDB.subread("test_subread2", key="b") == {"d": "e"}


def test_subwrite(env, use_compression, use_orjson, sort_keys, indent):
	name = "test_subwrite"
	j = {
		"b": {"0": 1},
		"c": {"d": "e"},
	}

	DDB.at(name).create(j, force_overwrite=True)
	with DDB.subsession(name, key="c", as_PathDict=True) as (session, task):
		task["f"] = lambda x: (x or 0) + 5
		session.write()
	assert DDB.subread(name, key="c") == {"d": "e", "f": 5}


	with DDB.subsession(name, key="b", as_PathDict=True) as (session, task):
		task["f"] = lambda x: (x or 0) + 2
		session.write()
	assert DDB.subread(name, key="f") == 2
