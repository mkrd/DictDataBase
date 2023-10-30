import json

import pytest

import dictdatabase as DDB
from dictdatabase import io_safe


def test_read(use_compression, use_orjson, indent):
	# Elicit read error
	DDB.config.use_orjson = True
	with pytest.raises(json.decoder.JSONDecodeError):
		with open(f"{DDB.config.storage_directory}/corrupted_json.json", "w") as f:
			f.write("This is not JSON")
		io_safe.read("corrupted_json")


def test_partial_read(use_compression, use_orjson, indent):
	assert io_safe.partial_read("nonexistent", key="none") is None


def test_write(use_compression, use_orjson, indent):
	with pytest.raises(TypeError):
		io_safe.write("nonexistent", lambda x: x)


def test_delete(use_compression, use_orjson, indent):
	DDB.at("to_be_deleted").create()
	DDB.at("to_be_deleted").delete()
	assert DDB.at("to_be_deleted").read() is None
