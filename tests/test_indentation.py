
import dictdatabase as DDB
import orjson
import json
from dictdatabase import utils, io_unsafe, config

data = {
	'a': 1,
	'b': {
		'c': 2,
		"cl": [1, "\\"],
		'd': {
			'e': 3,
			"el": [1, "\\"],
		}
	},
	"l": [1, "\\"],
}


def string_dump(db: dict):
	if not config.use_orjson:
		return json.dumps(db, indent=config.indent, sort_keys=config.sort_keys).encode()
	option = orjson.OPT_INDENT_2 if config.indent else 0
	option |= orjson.OPT_SORT_KEYS if config.sort_keys else 0
	return orjson.dumps(db, option=option)




def test_indentation(env, use_compression, use_orjson, sort_keys, indent):
	DDB.at("test_indentation").create(data, force_overwrite=True)

	with DDB.at("test_indentation", key="b").session() as (session, db_b):
		db_b["c"] = 3
		session.write()
	data["b"]["c"] = 3

	assert io_unsafe.read_bytes("test_indentation") == string_dump(data)

	with DDB.at("test_indentation", key="d").session() as (session, db_d):
		db_d["e"] = 4
		session.write()
	data["b"]["d"]["e"] = 4
	assert io_unsafe.read_bytes("test_indentation") == string_dump(data)
