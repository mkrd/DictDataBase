from dictdatabase import locking
import pytest
import threading
import time
from tests import TEST_DIR


def test_double_lock_exception(use_test_dir, use_compression):
	with pytest.raises(RuntimeError):
		with locking.ReadLock("db"):
			with locking.ReadLock("db"):
				pass


def test_get_lock_names(use_test_dir, use_compression):
	lock = locking.ReadLock("db")
	lock._lock()

	ls = locking.FileLocksSnapshot(lock.ddb_dir, "none", lock.need_path)
	assert ls.locks == []
	ls = locking.FileLocksSnapshot(lock.ddb_dir, "db", lock.need_path)
	assert len(ls.locks) == 1

	assert ls.locks[0].id == str(threading.get_native_id())
	assert ls.locks[0].time_ns == str(lock.time_ns)
	assert ls.locks[0].stage == "has"
	assert ls.locks[0].mode == "read"

	assert ls.any_has_locks
	assert not ls.any_write_locks
	assert not ls.any_has_write_locks

	lock._unlock()





def test_remove_orphaned_locks(use_test_dir):
	prev_config = locking.LOCK_TIMEOUT
	locking.LOCK_TIMEOUT = 0.1
	lock = locking.ReadLock("db")
	lock._lock()

	ls = locking.FileLocksSnapshot(lock.ddb_dir, "db", lock.need_path)
	assert len(ls.locks) == 1

	time.sleep(0.2)
	# Trigger the removal of orphaned locks
	ls = locking.FileLocksSnapshot(lock.ddb_dir, "db", lock.need_path)

	assert len(ls.locks) == 0
	locking.LOCK_TIMEOUT = prev_config


def test_AbstractLock(use_test_dir):
	l = locking.AbstractLock("test")
	with pytest.raises(NotImplementedError):
		l._lock()
