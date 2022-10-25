from unicodedata import name
import dictdatabase as DDB
import pytest
from tests.utils import make_complex_nested_random_dict


def test_non_existent_session(env):
	name = "test_non_existent_session"
	with pytest.raises(FileNotFoundError):
		with DDB.at(name).session() as (session, d):
			session.write()


def test_write(env, use_compression, use_orjson, sort_keys, indent):
	name = "test_write"
	d = make_complex_nested_random_dict(12, 6)
	DDB.at(name).create(d, force_overwrite=True)
	with DDB.at(name).session() as (session, dd):
		assert d == dd
		session.write()


def test_write_compression_switching(env, use_orjson, sort_keys, indent):
	name = "test_write_compression_switching"
	DDB.config.use_compression = False
	d = make_complex_nested_random_dict(12, 6)
	DDB.at(name).create(d, force_overwrite=True)
	with DDB.at(name).session() as (session, dd):
		assert d == dd
		session.write()
	assert DDB.at(name).read() == d
	DDB.config.use_compression = True
	with DDB.at(name).session() as (session, dd):
		assert d == dd
		session.write()
	assert DDB.at(name).read() == d
	DDB.config.use_compression = False
	with DDB.at(name).session() as (session, dd):
		assert d == dd
		session.write()
	assert DDB.at(name).read() == d
