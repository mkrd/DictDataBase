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


def test_read_lock_release():
	read_lock = locking.ReadLock("test_db")
	write_lock = locking.WriteLock("test_db")

	# Acquire and release a read lock
	with read_lock:
		pass

	# Now attempt to acquire a write lock
	with write_lock:
		assert write_lock.has_lock is not None

	read_lock._unlock()
	write_lock._unlock()


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


def test_lock_must_implement_lock_function():
	class BadLock(locking.AbstractLock):
		mode = "read"

	lock = BadLock("db")
	with pytest.raises(NotImplementedError):
		lock._lock()


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

	lock._unlock()

	locking.AQUIRE_LOCK_TIMEOUT, locking.LOCK_KEEP_ALIVE_TIMEOUT, locking.REMOVE_ORPHAN_LOCK_TIMEOUT = prev


def test_lock_keep_alive():
	prev = locking.AQUIRE_LOCK_TIMEOUT, locking.LOCK_KEEP_ALIVE_TIMEOUT, locking.REMOVE_ORPHAN_LOCK_TIMEOUT

	locking.LOCK_KEEP_ALIVE_TIMEOUT = 0.1
	locking.ALIVE_LOCK_MAX_AGE = 0.5

	lock = locking.ReadLock("test_lock_keep_alive")

	with lock:
		time.sleep(1.0)

	locking.AQUIRE_LOCK_TIMEOUT, locking.LOCK_KEEP_ALIVE_TIMEOUT, locking.REMOVE_ORPHAN_LOCK_TIMEOUT = prev
