import json

import pytest
from path_dict import pd

import dictdatabase as DDB


def test_subread(use_compression, use_orjson, indent):
	name = "test_subread"
	j = {
		"a": "Hello{}",
		"b": [0, 1],
		"c": {"d": "e"},
	}

	DDB.at(name).create(j, force_overwrite=True)

	assert DDB.at(name, key="a").read() == "Hello{}"
	assert DDB.at(name, where=lambda k, v: isinstance(v, list)).read() == {"b": [0, 1]}

	assert DDB.at(name, key="f").read() is None

	assert DDB.at(name, key="b").read() == [0, 1]
	assert DDB.at(name, key="c").read() == {"d": "e"}

	j2 = {"a": {"b": "c"}, "b": {"d": "e"}}
	DDB.at("test_subread2").create(j2, force_overwrite=True)
	assert DDB.at("test_subread2", key="b").read() == {"d": "e"}

	assert DDB.at("none", key="none").read() is None

	j3 = {"a": {"b": {"\\c\\": {"a": "a"}}}}
	DDB.at("test_subread3").create(j3, force_overwrite=True)
	assert DDB.at("test_subread3", key="a").read() == {"b": {"\\c\\": {"a": "a"}}}


def test_subwrite(use_compression, use_orjson, indent):
	name = "test_subwrite"
	j = {
		"b": {"0": 1},
		"c": {"d": "e"},
	}

	DDB.at(name).create(j, force_overwrite=True)
	with DDB.at(name, key="c").session(as_type=pd) as (session, task):
		task["f"] = lambda x: (x or 0) + 5
		session.write()
	assert DDB.at(name, key="c").read() == {"d": "e", "f": 5}

	with DDB.at(name, key="b").session(as_type=pd) as (session, task):
		task["f"] = lambda x: (x or 0) + 2
		session.write()

	assert DDB.at(name, key="f").read() is None

	with pytest.raises(KeyError):
		with DDB.at(name, key="none").session() as (session, key):
			session.write()


def test_write_file_where(use_compression, use_orjson, indent):
	name = "test_write_file_where"
	j = {
		"a": 1,
		"b": 20,
		"c": 3,
		"d": 40,
	}

	DDB.at(name).create(j, force_overwrite=True)

	with DDB.at(name, where=lambda k, v: v > 10).session() as (session, vals):
		vals.update({"b": 30, "d": 50, "e": 60})
		session.write()
	assert DDB.at(name).read() == {
		"a": 1,
		"b": 30,
		"c": 3,
		"d": 50,
		"e": 60,
	}


def test_dir_where(use_compression, use_orjson, indent):
	name = "test_dir_where"
	for i in range(5):
		DDB.at(name, i).create({"k": i}, force_overwrite=True)

	with DDB.at(name, "*", where=lambda k, v: v["k"] > 2).session() as (session, vals):
		for k, v in vals.items():
			v["k"] += 1
		session.write()
	assert DDB.at(name, "*").read() == {
		"0": {"k": 0},
		"1": {"k": 1},
		"2": {"k": 2},
		"3": {"k": 4},
		"4": {"k": 5},
	}
