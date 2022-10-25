import dictdatabase as DDB
import pytest
from tests.utils import make_complex_nested_random_dict


def test_non_existent(env, use_compression, use_orjson, sort_keys, indent):
	d = DDB.read("nonexistent")
	assert d is None


def test_create_and_read(env, use_compression, use_orjson, sort_keys, indent):
	d = make_complex_nested_random_dict(12, 6)
	DDB.create("test_create_and_read", db=d, force_overwrite=True)
	dd = DDB.read("test_create_and_read")
	assert d == dd


def test_read_compression_switching(env, use_orjson, sort_keys, indent):
	DDB.config.use_compression = False
	d = make_complex_nested_random_dict(12, 6)
	DDB.create("test", db=d, force_overwrite=True)
	DDB.config.use_compression = True
	dd = DDB.read("test")
	assert d == dd
	DDB.create("test", db=d, force_overwrite=True)
	DDB.config.use_compression = False
	dd = DDB.read("test")
	assert d == dd


def test_multiread(env, use_compression, use_orjson, sort_keys, indent):
	dl = []
	for i in range(3):
		dl += [make_complex_nested_random_dict(12, 6)]
		DDB.create(f"test_multiread/d{i}", db=dl[-1], force_overwrite=True)

	mr = DDB.multiread("test_multiread/*")
	mr2 = DDB.multiread("test_multiread", "*")
	assert mr == mr2
	mr = {k.replace("test_multiread/", ""): v for k, v in mr.items()}
	assert mr == {f"d{i}": dl[i] for i in range(3)}
