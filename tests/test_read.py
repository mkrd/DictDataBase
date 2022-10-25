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
