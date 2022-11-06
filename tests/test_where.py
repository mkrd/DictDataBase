import dictdatabase as DDB
from path_dict import PathDict
import pytest


def test_where(env, use_compression, use_orjson, sort_keys, indent):
    for i in range(10):
        DDB.at("test_select", i).create({"a": i}, force_overwrite=True)

    s = DDB.at("test_select/*", where=lambda k, v: v["a"] > 7).read()

    assert s == {"8": {"a": 8}, "9": {"a": 9}}

    with pytest.raises(KeyError):
        DDB.at("test_select/*", where=lambda k, v: v["b"] > 5).read()

    assert DDB.at("nonexistent/*", where=lambda k, v: v["a"] > 5).read() == {}

    assert DDB.at("nonexistent", where=lambda k, v: v["a"] > 5).read() is None

    s = DDB.at("test_select/*", where=lambda k, v: v.at("a").get()).read(as_type=PathDict)
