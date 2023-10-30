import json

import orjson
import pytest

import dictdatabase as DDB
from dictdatabase import config, io_bytes, io_unsafe, utils

data = {
	"a": 1,
	"b": {
		"c": 2,
		"cl": [1, "\\"],
		"d": {
			"e": 3,
			"el": [1, "\\"],
		},
	},
	"l": [1, "\\"],
}


def string_dump(db: dict):
	if not config.use_orjson:
		return json.dumps(db, indent=config.indent, sort_keys=True).encode()
	option = (orjson.OPT_INDENT_2 if config.indent else 0) | orjson.OPT_SORT_KEYS
	return orjson.dumps(db, option=option)


def test_indentation(use_compression, use_orjson, indent):
	DDB.at("test_indentation").create(data, force_overwrite=True)

	with DDB.at("test_indentation", key="b").session() as (session, db_b):
		db_b["c"] = 3
		session.write()
	data["b"]["c"] = 3

	assert io_bytes.read("test_indentation") == string_dump(data)

	# Accessing a key not at root level should raise an error
	with pytest.raises(KeyError):
		with DDB.at("test_indentation", key="d").session() as (session, db_d):
			session.write()
	assert io_bytes.read("test_indentation") == string_dump(data)
