import dictdatabase as DDB


def test_delete(env, use_compression):
    DDB.at("test_delete").create({"a": 1}, force_overwrite=True)
    assert DDB.at("test_delete").read() == {"a": 1}
    DDB.at("test_delete").delete()
    assert DDB.at("test_delete").read() is None



def test_delete_nonexistent():
    DDB.at("test_delete_nonexistent").delete()
