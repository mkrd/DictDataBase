import itertools

import orjson

from dictdatabase import byte_codes, utils


def test_seek_index_through_value_bytes():
	v = b'{"a": 1, "b": {}}'
	assert utils.seek_index_through_value_bytes(v, 5) == 7
	assert utils.seek_index_through_value_bytes(v, 6) == 7
	assert utils.seek_index_through_value_bytes(v, 13) == 16
	vc = b'{"a":1,"b":{}}'
	assert utils.seek_index_through_value_bytes(vc, 5) == 6
	assert utils.seek_index_through_value_bytes(vc, 11) == 13
	n = b'{"a": 1234, "b": {"c": 2}}'
	assert utils.seek_index_through_value_bytes(n, 5) == 10
	assert utils.seek_index_through_value_bytes(n, 6) == 10


def test_seek_index_through_value_bytes_2():
	def load_with_orjson(bytes, key):
		return orjson.loads(bytes)[key]

	def load_with_seeker(bytes, key):
		key_bytes = f'"{key}":'.encode()
		a_val_start = bytes.find(key_bytes) + len(key_bytes)
		if bytes[a_val_start] == byte_codes.SPACE:
			a_val_start += 1
		a_val_end = utils.seek_index_through_value_bytes(bytes, a_val_start)
		return orjson.loads(bytes[a_val_start:a_val_end])

	values = [
		# Lists
		[],
		[{}],
		[""],
		[1],
		[1, 2, 3],
		["xs", -123.3, "c"],
		[1, "xs", 2, "value", 3, "c"],
		[1, "xs", 2, "value", 3, "c", [1, 2, 3], [1, 2, 3], [1, 2, 3]],
		[{}, {}, {}],
		[{"xs": 1}, {"value": 2}, {"c": 3}],
		[{"xs": 1}, {"value": 2}, {"c": 3}, {"xs": 1}, {"value": 2}, {"c": 3}],
		[{"xs": 1}, {"value": 2}, {"c": 3}, {"xs": 1}, {"value": 2}, {"c": 3}, [1, 2, 3], [1, 2, 3], [1, 2, 3]],
		# Dicts
		{},
		{"": ""},
		{"x": []},
		{"xs": 1},
		{"xs": 1, "value": 2},
		{"xs": [], "value": {}},
		{"xs": -3.3, "value": ""},
		# Numbers
		1,
		1234,
		1.3,
		32.3,
		0,
		-1.3,
		-0,
		# Strings
		"",
		"a",
		"hello",
		"a\\b",
		"\\",
		"\\\\",
		'\\\\"',
		'\\"\\',
		'"\\\\',
		'"',
		'""',
		'""\\',
		'"\\"',
		'\\""',
		# Booleans
		True,
		None,
		False,
	]

	for indent, v1, v2 in itertools.product([False, True], values, values):
		option = orjson.OPT_SORT_KEYS | (orjson.OPT_INDENT_2 if indent else 0)
		json_bytes = orjson.dumps({"a": v1, "b": v2}, option=option)
		assert load_with_orjson(json_bytes, "a") == load_with_seeker(json_bytes, "a")
		assert load_with_orjson(json_bytes, "b") == load_with_seeker(json_bytes, "b")
