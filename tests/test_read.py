
import dictdatabase as DDB
import pytest
import json
from tests.utils import make_complex_nested_random_dict


def test_non_existent(env, use_compression, use_orjson, sort_keys, indent):
	d = DDB.at("nonexistent").read()
	assert d is None


def test_read_integrity():
	cases = [
		r'{"a": "\\", "b": 2}',
		r'{"a": "\\\\", "b": 2}',
		r'{"a": "\\\\\"", "b": 2}',
		r'{"a": "\\\"\\", "b": 2}',
		r'{"a": "\"\\\\", "b": 2}',
		r'{"a": "\"", "b": 2}',
		r'{"a": "\"\"", "b": 2}',
		r'{"a": "\"\"\\", "b": 2}',
		r'{"a": "\"\\\"", "b": 2}',
		r'{"a": "\\\"\"", "b": 2}',
	]

	for case in cases:
		with open(f"{DDB.config.storage_directory}/test_read_integrity.json", "w") as f:
			f.write(case)
		dd = DDB.at("test_read_integrity").read(key="a")
		assert dd == json.loads(case)["a"]






def test_create_and_read(env, use_compression, use_orjson, sort_keys, indent):
	name = "test_create_and_read"
	d = make_complex_nested_random_dict(12, 6)
	DDB.at(name).create(d, force_overwrite=True)
	dd = DDB.at(name).read()
	assert d == dd


def test_read_compression_switching(env, use_orjson, sort_keys, indent):
	name = "test_read_compression_switching"
	DDB.config.use_compression = False
	d = make_complex_nested_random_dict(12, 6)
	DDB.at(name).create(d, force_overwrite=True)
	DDB.config.use_compression = True
	dd = DDB.at(name).read()
	assert d == dd
	DDB.at(name).create(d, force_overwrite=True)
	DDB.config.use_compression = False
	dd = DDB.at(name).read()
	assert d == dd


def test_multiread(env, use_compression, use_orjson, sort_keys, indent):
	dl = []
	for i in range(3):
		dl += [make_complex_nested_random_dict(12, 6)]
		DDB.at(f"test_multiread/d{i}").create(dl[-1], force_overwrite=True)

	mr = DDB.at("test_multiread/*").read()
	mr2 = DDB.at("test_multiread", "*").read()
	assert mr == mr2
	mr = {k.replace("test_multiread/", ""): v for k, v in mr.items()}
	assert mr == {f"d{i}": dl[i] for i in range(3)}
