import dictdatabase as DDB

data = {
    "users": {
        "Ben": {"age": 30, "job": "Software Engineer", "password": 123},
        "Bob": {"age": 30, "job": "Plumbers"},
    },
    "admin": {"job": {"age": 30, "job": "Software Engineer"}},
}


def test_glom_searching():
    DDB.at("users").create(data, force_overwrite=True)
    assert DDB.at("users", key="users.Ben.job").read() == "Software Engineer"


def test_without_glom_searching():
    DDB.at("users").create(data, force_overwrite=True)
    assert DDB.at("users", key="Ben").read() == {
        "job": {"age": 30, "job": "Software Engineer"}
    }


def test_injection():
    DDB.at("users").create(data, force_overwrite=True)
    assert DDB.at("users", key="job").read() == {
        "job": {"age": 30, "job": "Software Engineer"}
    }
