import dictdatabase as DDB

data = {
    "users": {
        "Ben": {"age": 30, "job": "Software Engineer"},
        "Bob": {"age": 30, "job": "Plumbers"},
    },
    "Ben": {"job": {"age": 30, "job": "Software Engineer"}},
}


def test_glom_searching():
    DDB.at("users").create(data, force_overwrite=True)
    assert DDB.at("users", key="users.Ben.job").read() == "Software Engineer"


def test_without_glom_searching():
    DDB.at("users").create(data, force_overwrite=True)
    assert DDB.at("users", key="Ben").read() == {
        "job": {"age": 30, "job": "Software Engineer"}
    }


def test_glom_searching_if_key_not_exists():
    DDB.at("users").create(data, force_overwrite=True)
    assert DDB.at("users", key="users.Job.Ben").read() is None


def test_glom_searching_if_subkey_not_exists():
    DDB.at("users").create(data, force_overwrite=True)
    assert DDB.at("users", key="users.Ben.SUBKEYNOTEXISTS").read() is None
