import dictdatabase as DDB
from path_dict import pd
import json
import pytest


def test_subread(env, use_compression, use_orjson, sort_keys, indent):
	name = "test_subread"
	j = {
		"a": "Hello{}",
		"b": [0, 1],
		"c": {"d": "e"},
	}

	DDB.at(name).create(j, force_overwrite=True)

	assert DDB.at(name).read("a") == "Hello{}"

	with pytest.raises(KeyError):
		DDB.at(name).read("f")

	assert DDB.at(name).read("b") == [0, 1]
	assert DDB.at(name).read("c") == {"d": "e"}

	j2 = {"a": {"b": "c"}, "b": {"d": "e"}}
	DDB.at("test_subread2").create(j2, force_overwrite=True)
	assert DDB.at("test_subread2").read("b") == {"d": "e"}

	assert DDB.at("none").read("none") is None

	j3 = {"a": {"b": {"\\c\\": {"a": "a"}}}}
	DDB.at("test_subread3").create(j3, force_overwrite=True)
	assert DDB.at("test_subread3").read("a") == {"b": {"\\c\\": {"a": "a"}}}


def test_subwrite(env, use_compression, use_orjson, sort_keys, indent):
	name = "test_subwrite"
	j = {
		"b": {"0": 1},
		"c": {"d": "e"},
	}

	DDB.at(name).create(j, force_overwrite=True)
	with DDB.at(name).session("c", as_type=pd) as (session, task):
		task["f"] = lambda x: (x or 0) + 5
		session.write()
	assert DDB.at(name).read("c") == {"d": "e", "f": 5}

	with DDB.at(name).session("b", as_type=pd) as (session, task):
		task["f"] = lambda x: (x or 0) + 2
		session.write()
	assert DDB.at(name).read("f") == 2
