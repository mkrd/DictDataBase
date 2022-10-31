from dictdatabase.locking import make_lock_path
from tests import TEST_DIR


def test_make_lock_path():
    # Testing the function path_str.
    assert "./" + str(make_lock_path("db", "1", 2, "3", "4")) == f"{TEST_DIR}/.ddb/db.1.2.3.4.lock"
    assert "./" + str(make_lock_path("db/nest", "1", 2, "3", "4")) == f"{TEST_DIR}/.ddb/db/nest.1.2.3.4.lock"
