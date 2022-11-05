import dictdatabase as DDB
from path_dict import PathDict
import pytest


def test_select(env, use_compression, use_orjson, sort_keys, indent):
    for i in range(10):
        DDB.at("test_select", i).create({"a": i}, force_overwrite=True)

    s = DDB.at("test_select/*").select(lambda x: x["a"] > 7)

    assert s == {"8": {"a": 8}, "9": {"a": 9}}

    with pytest.raises(ValueError):
        DDB.at("test_select").select(lambda x: x["a"] > 5)

    s = DDB.at("test_select/*").select(lambda x: x.at("a").get(), as_type=PathDict)
