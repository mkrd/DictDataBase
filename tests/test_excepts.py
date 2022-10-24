import dictdatabase as DDB
import pytest


def test_except_during_open_session(use_compression, use_orjson, sort_keys, indent):
	d = {"test": "value"}
	DDB.create("test", db=d, force_overwrite=True)
	with pytest.raises(RuntimeError):
		with DDB.session("test") as (session, test):
			raise RuntimeError("Any Exception")



def test_except_on_save_unserializable(use_compression, use_orjson, sort_keys, indent):
	with pytest.raises(TypeError):
		d = {"test": "value"}
		DDB.create("test", db=d, force_overwrite=True)
		with DDB.session("test", as_PathDict=True) as (session, test):
			test["test"] = {"key": {1, 2}}
			session.write()
		assert False


def test_except_on_session_in_session(use_compression, use_orjson, sort_keys, indent):
	d = {"test": "value"}
	DDB.create("test", db=d, force_overwrite=True)
	with pytest.raises(RuntimeError):
		with DDB.session("test", as_PathDict=True) as (session, test):
			with DDB.session("test", as_PathDict=True) as (session2, test2):
				pass
