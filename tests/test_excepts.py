import pytest
from path_dict import pd

import dictdatabase as DDB
from dictdatabase import io_bytes, utils


def test_except_during_open_session(use_compression, use_orjson, indent):
	name = "test_except_during_open_session"
	d = {"test": "value"}
	DDB.at(name).create(d, force_overwrite=True)
	with pytest.raises(RuntimeError):
		with DDB.at(name).session() as (session, test):
			raise RuntimeError("Any Exception")


def test_except_on_save_unserializable(use_compression, use_orjson, indent):
	name = "test_except_on_save_unserializable"
	with pytest.raises(TypeError):
		d = {"test": "value"}
		DDB.at(name).create(d, force_overwrite=True)
		with DDB.at(name).session(as_type=pd) as (session, test):
			test["test"] = {"key": {1, 2}}
			session.write()


def test_except_on_save_unserializable_in_multisession(use_compression, use_orjson, indent):
	name = "test_except_on_save_unserializable_in_multisession"
	with pytest.raises(TypeError):
		d = {"test": "value"}
		DDB.at(name, "1").create(d, force_overwrite=True)
		DDB.at(name, "2").create(d, force_overwrite=True)
		with DDB.at(name, "*").session(as_type=pd) as (session, test):
			test["1"]["test"] = {"key": {1, 2}}
			session.write()


def test_except_on_session_in_session(use_compression, use_orjson, indent):
	name = "test_except_on_session_in_session"
	d = {"test": "value"}
	DDB.at(name).create(d, force_overwrite=True)
	with pytest.raises(RuntimeError):
		with DDB.at(name).session(as_type=pd) as (session, test):
			with DDB.at(name).session(as_type=pd) as (session2, test2):
				pass


def test_except_on_write_outside_session(use_compression, use_orjson, indent):
	with pytest.raises(PermissionError):
		s = DDB.at("test_except_on_write_outside_session").session()
		s.write()


def test_wildcard_and_subkey_except(use_compression, use_orjson, indent):
	with pytest.raises(TypeError):
		DDB.at("test_wildcard_and_subkey_except/*", key="key").read()


def test_utils_invalid_json_except():
	with pytest.raises(TypeError):
		utils.seek_index_through_value_bytes(b"{This is not { JSON", 0)


def test_bytes_write_except():
	# It is not allowed to specify a start index when compression is used.
	with pytest.raises(RuntimeError):
		DDB.config.use_compression = True
		io_bytes.write("any", b"any", start=1)
