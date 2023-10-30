import pytest

import dictdatabase as DDB


def test_exists(use_compression, use_orjson, indent):
	DDB.at("test_exists").create({"a": 1}, force_overwrite=True)
	assert DDB.at("test_exists").exists()
	assert not DDB.at("test_exists/nonexistent").exists()
	assert DDB.at("test_exists", key="a").exists()
	assert not DDB.at("test_exists", key="b").exists()
	with pytest.raises(RuntimeError):
		DDB.at("test_exists", where=lambda k, v: True).exists()
