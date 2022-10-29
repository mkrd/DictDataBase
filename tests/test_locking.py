from dictdatabase.locking import path_str
from tests import TEST_DIR


def test_path_str():
    # Testing the function path_str.
    assert path_str("db", "1", 1, "1") == f"{TEST_DIR}/.ddb/db.1.1.1.lock"
    assert path_str("db/nest", "1", 1, "1") == f"{TEST_DIR}/.ddb/db/nest.1.1.1.lock"
