import pytest
from path_dict import pd

import dictdatabase as DDB
from tests.utils import make_complex_nested_random_dict


def test_non_existent_session(use_compression, use_orjson, indent):
	name = "test_non_existent_session"
	with pytest.raises(FileNotFoundError):
		with DDB.at(name).session() as (session, d):
			session.write()


def test_write(use_compression, use_orjson, indent):
	name = "test_write"
	d = make_complex_nested_random_dict(12, 6)
	DDB.at(name).create(d, force_overwrite=True)
	with DDB.at(name).session() as (session, dd):
		assert d == dd
		session.write()


def test_write_compression_switching(use_orjson, indent):
	name = "test_write_compression_switching"
	DDB.config.use_compression = False
	d = make_complex_nested_random_dict(12, 6)
	DDB.at(name).create(d, force_overwrite=True)
	with DDB.at(name).session() as (session, dd):
		assert d == dd
		session.write()
	assert DDB.at(name).read() == d
	DDB.config.use_compression = True
	with DDB.at(name).session(as_type=pd) as (session, dd):
		assert d == dd.get()
		session.write()
	assert DDB.at(name).read() == d
	DDB.config.use_compression = False
	with DDB.at(name).session() as (session, dd):
		assert d == dd
		session.write()
	assert DDB.at(name).read() == d


def test_multi_session(use_compression, use_orjson, indent):
	a = {"a": 1}
	b = {"b": 2}

	DDB.at("test_multi_session/d1").create(a, force_overwrite=True)
	DDB.at("test_multi_session/d2").create(b, force_overwrite=True)

	with DDB.at("test_multi_session/*").session() as (session, d):
		assert d == {"d1": a, "d2": b}
		session.write()
	assert DDB.at("test_multi_session/*").read() == {"d1": a, "d2": b}


def test_write_wildcard_key_except(use_compression, use_orjson, indent):
	with pytest.raises(TypeError):
		with DDB.at("test/*", key="any").session() as (session, d):
			pass
