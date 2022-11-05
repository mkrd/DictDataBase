from dictdatabase import locking
import pytest
import threading
import time
from tests import TEST_DIR


def test_double_lock_exception(env, use_compression):
	with pytest.raises(RuntimeError):
		with locking.ReadLock("db"):
			with locking.ReadLock("db"):
				pass


def test_get_lock_names(env, use_compression):
	lock = locking.ReadLock("db")
	lock._lock()
	assert locking.get_lock_file_names(lock.ddb_dir, "none") == []
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db")) == 1
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db", id=str(threading.get_native_id()))) == 1
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db", id="none")) == 0
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db", time_ns=lock.time_ns)) == 1
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db", time_ns="none")) == 0
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db", stage="has")) == 1
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db", stage="none")) == 0
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db", mode="read")) == 1
	assert len(locking.get_lock_file_names(lock.ddb_dir, "db", mode="none")) == 0
	lock._unlock()


def test_count_lock_files(env, use_compression):
	lock = locking.ReadLock("db")
	lock._lock()
	assert locking.get_lock_file_names(lock.ddb_dir, "none") == []
	assert locking.count_lock_files(lock.ddb_dir, "db") == 1
	assert locking.count_lock_files(lock.ddb_dir, "db", id=str(threading.get_native_id())) == 1
	assert locking.count_lock_files(lock.ddb_dir, "db", id="none") == 0
	assert locking.count_lock_files(lock.ddb_dir, "db", time_ns=lock.time_ns) == 1
	assert locking.count_lock_files(lock.ddb_dir, "db", time_ns="none") == 0
	assert locking.count_lock_files(lock.ddb_dir, "db", stage="has") == 1
	assert locking.count_lock_files(lock.ddb_dir, "db", stage="none") == 0
	assert locking.count_lock_files(lock.ddb_dir, "db", mode="read") == 1
	assert locking.count_lock_files(lock.ddb_dir, "db", mode="none") == 0
	lock._unlock()


def test_remove_orphaned_locks(env):
	prev_config = locking.LOCK_TIMEOUT
	locking.LOCK_TIMEOUT = 0.1
	lock = locking.ReadLock("db")
	lock._lock()
	time.sleep(0.2)
	assert locking.count_lock_files(lock.ddb_dir, "db") == 1
	lock.remove_orphaned_locks()
	assert locking.count_lock_files(lock.ddb_dir, "db") == 0
	locking.LOCK_TIMEOUT = prev_config


def test_AbstractLock(env):
	l = locking.AbstractLock("test")
	with pytest.raises(NotImplementedError):
		l._lock()
