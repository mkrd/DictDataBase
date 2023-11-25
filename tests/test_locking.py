import threading
import time

import pytest

from dictdatabase import locking


def test_lock_release():
	lock = locking.WriteLock("db_release")

	with lock:
		pass  # Lock should be released here

	# Now, another lock should be able to be acquired
	with locking.WriteLock("db_release"):
		pass


def test_orphaned_lock_timeout():
	prev_timeout = locking.LOCK_TIMEOUT
	locking.LOCK_TIMEOUT = 0.1
	lock = locking.WriteLock("db_orphaned")

	lock._lock()
	time.sleep(0.2)

	# Trigger the removal of orphaned locks
	ls = locking.FileLocksSnapshot(lock.need_lock)
	assert len(ls.locks) == 0

	locking.LOCK_TIMEOUT = prev_timeout


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
	assert int(ls.locks[0].time_ns) >= int(lock.need_lock.time_ns)
	assert ls.locks[0].stage == "has"
	assert ls.locks[0].mode == "read"

	assert ls.any_has_locks
	assert not ls.any_write_locks
	assert not ls.any_has_write_locks

	lock._unlock()


def test_remove_orphaned_locks():
	# SLEEP_TIMEOUT = 0.001
	# LOCK_KEEP_ALIVE_TIMEOUT = 0.001
	# REMOVE_ORPHAN_LOCK_TIMEOUT = 20.0  # Duration to wait before considering a lock as orphaned.
	# AQUIRE_LOCK_TIMEOUT = 60.0

	prev = locking.AQUIRE_LOCK_TIMEOUT, locking.LOCK_KEEP_ALIVE_TIMEOUT, locking.REMOVE_ORPHAN_LOCK_TIMEOUT

	locking.AQUIRE_LOCK_TIMEOUT = 10.0
	locking.LOCK_KEEP_ALIVE_TIMEOUT = 1.0
	locking.REMOVE_ORPHAN_LOCK_TIMEOUT = 0.1
	lock = locking.ReadLock("test_remove_orphaned_locks")
	lock._lock()

	ls = locking.FileLocksSnapshot(lock.need_lock)
	assert len(ls.locks) >= 1  ## The one lock or two if currently in keep alive handover

	time.sleep(0.2)
	# Trigger the removal of orphaned locks
	ls = locking.FileLocksSnapshot(lock.need_lock)

	assert len(ls.locks) == 0

	locking.AQUIRE_LOCK_TIMEOUT, locking.LOCK_KEEP_ALIVE_TIMEOUT, locking.REMOVE_ORPHAN_LOCK_TIMEOUT = prev
