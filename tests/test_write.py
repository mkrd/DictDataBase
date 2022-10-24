import dictdatabase as DDB
import pytest


def test_non_existent(use_compression, use_orjson, sort_keys, indent):
	with pytest.raises(FileNotFoundError):
		with DDB.session("nonexistent", as_PathDict=True) as (session, d):
			session.write()



def test_write(use_compression, use_orjson, sort_keys, indent):
	d = {"test": "value"}
	DDB.create("test", db=d, force_overwrite=True)
	with DDB.session("test", as_PathDict=True) as (session, dd):
		assert d == dd.dict
		session.write()


def test_write_compression_switching(use_orjson, sort_keys, indent):
	DDB.config.use_compression = False
	d = {"test": "value"}
	DDB.create("test", db=d, force_overwrite=True)
	with DDB.session("test", as_PathDict=True) as (session, dd):
		assert d == dd.dict
		session.write()
	DDB.config.use_compression = True
	with DDB.session("test", as_PathDict=True) as (session, dd):
		assert d == dd.dict
		session.write()
	DDB.config.use_compression = False
	with DDB.session("test", as_PathDict=True) as (session, dd):
		assert d == dd.dict
		session.write()
