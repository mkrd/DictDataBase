import json

import pytest

import dictdatabase as DDB
from tests.utils import make_complex_nested_random_dict


def test_non_existent(use_compression, use_orjson, indent):
	d = DDB.at("nonexistent").read()
	assert d is None


def test_file_exists_error(use_compression, use_orjson, indent):
	with open(f"{DDB.config.storage_directory}/test_file_exists_error.json", "w") as f:
		f.write("")
	with open(f"{DDB.config.storage_directory}/test_file_exists_error.ddb", "w") as f:
		f.write("")
	with pytest.raises(FileExistsError):
		DDB.at("test_file_exists_error").read()


def test_invalid_params(use_compression, use_orjson, indent):
	with pytest.raises(TypeError):
		DDB.at("test_invalid_params", key="any", where=lambda k, v: True).read()


def test_read_integrity(use_compression, use_orjson, indent):
	cases = [
		r'{"a": "\\", "b": 0}',
		r'{"a": "\\\\", "b": 1234}',
		r'{"a": "\\\\\"", "b": 1234}',
		r'{"a": "\\\"\\", "b": 1234}',
		r'{"a": "\"\\\\", "b": 1234}',
		r'{"a": "\"", "b": 1234}',
		r'{"a": "\"\"", "b": 1234}',
		r'{"a": "\"\"\\", "b": 1234}',
		r'{"a": "\"\\\"", "b": 1234}',
		r'{"a": "\\\"\"", "b": 1234}',
	]

	for case in cases:
		with open(f"{DDB.config.storage_directory}/test_read_integrity.json", "w") as f:
			f.write(case)
		key_a = DDB.at("test_read_integrity", key="a").read()
		key_b = DDB.at("test_read_integrity", key="b").read()
		assert key_a == json.loads(case)["a"]
		assert key_b == json.loads(case)["b"]


def test_create_and_read(use_compression, use_orjson, indent):
	name = "test_create_and_read"
	d = make_complex_nested_random_dict(12, 6)
	DDB.at(name).create(d, force_overwrite=True)
	dd = DDB.at(name).read()
	assert d == dd


def test_read_compression_switching(use_orjson, indent):
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


def test_multiread(use_compression, use_orjson, indent):
	dl = []
	for i in range(3):
		dl += [make_complex_nested_random_dict(12, 6)]
		DDB.at(f"test_multiread/d{i}").create(dl[-1], force_overwrite=True)

	mr = DDB.at("test_multiread/*").read()
	mr2 = DDB.at("test_multiread", "*").read()
	assert mr == mr2
	mr = {k.replace("test_multiread/", ""): v for k, v in mr.items()}
	assert mr == {f"d{i}": dl[i] for i in range(3)}
