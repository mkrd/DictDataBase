
import dictdatabase as DDB


def a_create():
    d = {"key1": "val1", "key2": 2, "key3": [1, "2", [3, 3]]}
    for i in range(4):
        d = {f"key{i}{j}": d for j in range(20)}
    # About 22MB
    DDB.create("_test_big_db", db=d)


def b_read():
    d = DDB.read("_test_big_db")


def c_open_session():
    with DDB.session("_test_big_db") as (session, d):
        pass


def d_open_session_and_write():
    with DDB.session("_test_big_db") as (session, d):
        session.write()
