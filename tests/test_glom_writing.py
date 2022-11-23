import pytest

import dictdatabase as DDB

data = {
    "users": {
        "Ben": {"age": 30, "job": "Software Engineer"},
        "Bob": {"age": 30, "job": "Plumbers"},
    },
    "Ben": {"job": {"age": 30, "job": "Software Engineer"}},
}


def test_glom_writing():
    DDB.at("users").create(data, force_overwrite=True)
    with DDB.at("users", key="users.Ben").session() as (session, purchase):
        purchase["status"] = "cancelled"
        session.write()
    assert DDB.at("users", key="users.Ben.status").read() == "cancelled"


def test_glom_writing_sub_key_not_exists():
    DDB.at("users").create(data, force_overwrite=True)
    with pytest.raises(KeyError):
        with DDB.at("users", key="users.SUBKEY").session() as (session, purchase):
            purchase["status"] = "cancelled"
            session.write()
