import pytest

import dictdatabase as DDB


def test_delete(use_compression, use_orjson, indent):
	DDB.at("test_delete").create({"a": 1}, force_overwrite=True)
	assert DDB.at("test_delete").read() == {"a": 1}
	DDB.at("test_delete").delete()
	assert DDB.at("test_delete").read() is None

	with pytest.raises(RuntimeError):
		DDB.at("test_delete", where=lambda k, v: True).delete()

	with pytest.raises(RuntimeError):
		DDB.at("test_delete", key="any").delete()


def test_delete_nonexistent(use_compression, use_orjson, indent):
	DDB.at("test_delete_nonexistent").delete()
