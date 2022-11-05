from dictdatabase import locking
import pytest
import threading
import time
from tests import TEST_DIR


def test_make_lock_path(env, use_compression):
	# Testing the function path_str.
	assert str(locking.make_lock_path("db", "1", 2, "3", "4")) == f"{TEST_DIR}/.ddb/db.1.2.3.4.lock"
	assert str(locking.make_lock_path("db/nest", "1", 2, "3", "4")) == f"{TEST_DIR}/.ddb/db/nest.1.2.3.4.lock"


def test_double_lock_exception(env, use_compression):
	with pytest.raises(RuntimeError):
		with locking.ReadLock("db"):
			with locking.ReadLock("db"):
				pass


def test_get_lock_names(env, use_compression):
	lock = locking.ReadLock("db")
	lock._lock()
	assert locking.get_lock_file_names("none") == []
	assert len(locking.get_lock_file_names("db")) == 1
	assert len(locking.get_lock_file_names("db", id=str(threading.get_native_id()))) == 1
	assert len(locking.get_lock_file_names("db", id="none")) == 0
	assert len(locking.get_lock_file_names("db", time_ns=lock.time_ns)) == 1
	assert len(locking.get_lock_file_names("db", time_ns="none")) == 0
	assert len(locking.get_lock_file_names("db", stage="has")) == 1
	assert len(locking.get_lock_file_names("db", stage="none")) == 0
	assert len(locking.get_lock_file_names("db", mode="read")) == 1
	assert len(locking.get_lock_file_names("db", mode="none")) == 0
	lock._unlock()


def test_count_lock_files(env, use_compression):
	lock = locking.ReadLock("db")
	lock._lock()
	assert locking.get_lock_file_names("none") == []
	assert locking.count_lock_files("db") == 1
	assert locking.count_lock_files("db", id=str(threading.get_native_id())) == 1
	assert locking.count_lock_files("db", id="none") == 0
	assert locking.count_lock_files("db", time_ns=lock.time_ns) == 1
	assert locking.count_lock_files("db", time_ns="none") == 0
	assert locking.count_lock_files("db", stage="has") == 1
	assert locking.count_lock_files("db", stage="none") == 0
	assert locking.count_lock_files("db", mode="read") == 1
	assert locking.count_lock_files("db", mode="none") == 0
	lock._unlock()


def test_remove_orphaned_locks(env):
	prev_config = locking.LOCK_TIMEOUT
	locking.LOCK_TIMEOUT = 0.1
	lock = locking.ReadLock("db")
	lock._lock()
	time.sleep(0.2)
	assert locking.count_lock_files("db") == 1
	locking.remove_orphaned_locks("db")
	assert locking.count_lock_files("db") == 0
	locking.LOCK_TIMEOUT = prev_config


def test_AbstractLock(env):
	l = locking.AbstractLock("test")
	with pytest.raises(NotImplementedError):
		l._lock()
