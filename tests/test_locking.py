from dictdatabase.locking import path_str
from tests import TEST_DIR


def test_path_str():
    # Testing the function path_str.
    assert path_str("db", id="1", time_ns=2, stage="3", mode="4") == f"{TEST_DIR}/.ddb/db.1.2.3.4.lock"
    assert path_str("db/nest", id="1", time_ns=2, stage="3", mode="4") == f"{TEST_DIR}/.ddb/db/nest.1.2.3.4.lock"
