import itertools
import orjson
from dictdatabase import utils, io_unsafe, byte_codes


def test_seek_index_through_value_bytes(use_test_dir):
	v = b'{"a": 1, "b": {}}'
	vc = b'{"a":1,"b":{}}'

	assert utils.seek_index_through_value_bytes(v, 5) == 7
	assert utils.seek_index_through_value_bytes(v, 6) == 7
	assert utils.seek_index_through_value_bytes(vc, 5) == 6

	assert utils.seek_index_through_value_bytes(v, 13) == 16
	assert utils.seek_index_through_value_bytes(vc, 11) == 13


	n = b'{"a": 1234, "b": {"c": 2}}'
	assert utils.seek_index_through_value_bytes(n, 5) == 10
	assert utils.seek_index_through_value_bytes(n, 6) == 10


test_seek_index_through_value_bytes(0)





def load_with_orjson(bytes, key):
	# print("load with orjson", bytes)
	return orjson.loads(bytes)[key]


def load_with_seeker(bytes, key):
	key_bytes = f"\"{key}\":".encode()
	a_val_start = bytes.find(key_bytes) + len(key_bytes)
	if bytes[a_val_start] == byte_codes.SPACE:
		a_val_start += 1
	a_val_end = utils.seek_index_through_value_bytes(bytes, a_val_start)
	return orjson.loads(bytes[a_val_start:a_val_end])


def test_seek_index_through_value_bytes_2(use_test_dir):


	def orjson_dump_with_indent(data):
		return orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)

	def orjson_dump_without_indent(data):
		return orjson.dumps(data, option=orjson.OPT_SORT_KEYS)

	orjson_dump_settings = [orjson_dump_with_indent, orjson_dump_without_indent]

	values = [
		# Lists
		[],
		[1, 2, 3],
		["xs", "value", "c"],
		[1, "xs", 2, "value", 3, "c"],
		[1, "xs", 2, "value", 3, "c", [1, 2, 3], [1, 2, 3], [1, 2, 3]],
		[{}, {}, {}],
		[{"xs": 1}, {"value": 2}, {"c": 3}],
		[{"xs": 1}, {"value": 2}, {"c": 3}, {"xs": 1}, {"value": 2}, {"c": 3}],
		[{"xs": 1}, {"value": 2}, {"c": 3}, {"xs": 1}, {"value": 2}, {"c": 3}, [1, 2, 3], [1, 2, 3], [1, 2, 3]],
		# Dicts
		{},
		{"xs": 1},
		{"xs": 1, "value": 2},
		{"xs": 1, "value": 2, "c": 3},
		{"xs": []},
		{"xs": [], "value": []},
		{"xs": -3.3, "value": ""},
		# Numbers
		1,
		1234,
		1.3,
		-1.3,
		32.3,
		0,
		-0,
		# Strings
		"",
		"a",
		"hello",
		"a\\b",
		"\\",
		"\\\\",
		"\\\\\"",
		"\\\"\\",
		"\"\\\\",
		"\"",
		"\"\"",
		"\"\"\\",
		"\"\\\"",
		"\\\"\"",
	]

	for dumper, v1, v2 in itertools.product(orjson_dump_settings, values, values):

		obj = {"a": v1, "b": v2}

		json_bytes = dumper(obj)


		a_from_orjson = load_with_orjson(json_bytes, "a")
		a_from_seeker = load_with_seeker(json_bytes, "a")

		b_from_orjson = load_with_orjson(json_bytes, "b")
		b_from_seeker = load_with_seeker(json_bytes, "b")

		# print("obj", obj)
		# print("a_from_orjson", a_from_orjson)
		# print("a_from_seeker", a_from_seeker)
		assert a_from_orjson == a_from_seeker
		# print("b_from_orjson", b_from_orjson)
		# print("b_from_seeker", b_from_seeker)
		assert b_from_orjson == b_from_seeker



test_seek_index_through_value_bytes_2(0)
