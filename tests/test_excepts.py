import dictdatabase as DDB
import pytest


def test_except_during_open_session(env, use_compression, use_orjson, sort_keys, indent):
	d = {"test": "value"}
	DDB.create("test_except_during_open_session", db=d, force_overwrite=True)
	with pytest.raises(RuntimeError):
		with DDB.session("test_except_during_open_session") as (session, test):
			raise RuntimeError("Any Exception")



def test_except_on_save_unserializable(env, use_compression, use_orjson, sort_keys, indent):
	with pytest.raises(TypeError):
		d = {"test": "value"}
		DDB.create("test_except_on_save_unserializable", db=d, force_overwrite=True)
		with DDB.session("test_except_on_save_unserializable", as_PathDict=True) as (session, test):
			test["test"] = {"key": {1, 2}}
			session.write()


def test_except_on_session_in_session(env, use_compression, use_orjson, sort_keys, indent):
	d = {"test": "value"}
	DDB.create("test_except_on_session_in_session", db=d, force_overwrite=True)
	with pytest.raises(RuntimeError):
		with DDB.session("test_except_on_session_in_session", as_PathDict=True) as (session, test):
			with DDB.session("test_except_on_session_in_session", as_PathDict=True) as (session2, test2):
				pass
