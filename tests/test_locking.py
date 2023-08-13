from dictdatabase import locking
import pytest
import threading
import time


def test_double_lock_exception(use_compression):
	name = "test_double_lock_exception"
	with pytest.raises(RuntimeError):
		with locking.ReadLock(name):
			with locking.ReadLock(name):
				pass

	ls = locking.FileLocksSnapshot(locking.ReadLock(name).need_lock)
	assert len(ls.locks) == 0


def test_get_lock_names(use_compression):
	lock = locking.ReadLock("db")
	lock._lock()

	ls = locking.FileLocksSnapshot(locking.ReadLock("none").need_lock)
	assert ls.locks == []
	ls = locking.FileLocksSnapshot(lock.need_lock)
	assert len(ls.locks) == 1

	assert ls.locks[0].id == str(threading.get_native_id())
	assert ls.locks[0].time_ns == str(lock.need_lock.time_ns)
	assert ls.locks[0].stage == "has"
	assert ls.locks[0].mode == "read"

	assert ls.any_has_locks
	assert not ls.any_write_locks
	assert not ls.any_has_write_locks

	lock._unlock()





def test_remove_orphaned_locks():
	prev_config = locking.LOCK_TIMEOUT
	locking.LOCK_TIMEOUT = 0.1
	lock = locking.ReadLock("test_remove_orphaned_locks")
	lock._lock()

	ls = locking.FileLocksSnapshot(lock.need_lock)
	assert len(ls.locks) == 1

	time.sleep(0.2)
	# Trigger the removal of orphaned locks
	ls = locking.FileLocksSnapshot(lock.need_lock)

	assert len(ls.locks) == 0
	locking.LOCK_TIMEOUT = prev_config
