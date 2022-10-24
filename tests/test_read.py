import dictdatabase as DDB
import pytest


def test_non_existent(use_compression, use_orjson, sort_keys, indent):
	d = DDB.read("nonexistent")
	assert d is None


def test_cread_read(use_compression, use_orjson, sort_keys, indent):
	d = {"test": "value", "test2": [0, {"test3": "value3"}]}
	DDB.create("test", db=d, force_overwrite=True)
	dd = DDB.read("test")
	assert d == dd


def test_read_compression_switching(use_orjson, sort_keys, indent):
	DDB.config.use_compression = False
	d = {"test": "value"}
	DDB.create("test", db=d, force_overwrite=True)
	DDB.config.use_compression = True
	dd = DDB.read("test")
	assert d == dd
	DDB.create("test", db=d, force_overwrite=True)
	DDB.config.use_compression = False
	dd = DDB.read("test")
	assert d == dd
