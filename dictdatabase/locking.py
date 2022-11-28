from __future__ import annotations
import threading
import time
import os
from . import config

# Design decisions:
# - Do not use pathlib, because it is slower than os

SLEEP_TIMEOUT = 0.001

# If a process crashes and doesn't remove its locks, remove them after a timeout
LOCK_TIMEOUT = 30.0


def os_touch(path: str):
	"""
		Like touch, but works on Windows.
	"""
	mode = 0o666
	flags = os.O_CREAT | os.O_WRONLY | os.O_EXCL
	fd = os.open(path, flags, mode)
	os.close(fd)


class LockFileMeta:

	__slots__ = ("ddb_dir", "name", "id", "time_ns", "stage", "mode", "path")

	ddb_dir: str
	id: str
	name: str
	time_ns: str
	stage: str
	mode: str
	path: str

	def __init__(self, ddb_dir, name, id, time_ns, stage, mode):
		self.ddb_dir, self.name, self.id = ddb_dir, name, id
		self.time_ns, self.stage, self.mode = time_ns, stage, mode
		lock_file = f"{name}.{id}.{time_ns}.{stage}.{mode}.lock"
		self.path = os.path.join(ddb_dir, lock_file)


class FileLocksSnapshot:

	__slots__ = ("any_has_locks", "any_write_locks", "any_has_write_locks", "locks")

	any_has_locks: bool
	any_write_locks: bool
	any_has_write_locks: bool
	locks: list[LockFileMeta]

	def __init__(self, need_lock: LockFileMeta):
		self.any_has_locks, self.any_write_locks = False, False
		self.any_has_write_locks, self.locks = False, []

		for file_name in os.listdir(need_lock.ddb_dir):
			if not file_name.endswith(".lock"):
				continue
			name, id, time_ns, stage, mode, _ = file_name.split(".")
			if name != need_lock.name:
				continue

			lock_meta = LockFileMeta(need_lock.ddb_dir, name, id, time_ns, stage, mode)

			# Remove orphaned locks
			if lock_meta.path != need_lock.path:
				lock_age = time.monotonic_ns() - int(lock_meta.time_ns)
				if lock_age > LOCK_TIMEOUT * 1_000_000_000:
					os.unlink(lock_meta.path)
					print(f"Removed orphaned lock ({lock_meta.path})")
					continue

			self.locks.append(lock_meta)

			# Lock existence
			if lock_meta.stage == "has":
				self.any_has_locks = True
				if lock_meta.mode == "write":
					self.any_has_write_locks = True
			if lock_meta.mode == "write":
				self.any_write_locks = True

	def exists(self, l: LockFileMeta) -> bool:
		return any(x.id == l.id and x.stage == l.stage and x.mode == l.mode for x in self.locks)

	def oldest_need(self, need_lock: LockFileMeta) -> bool:
		# len(need_locks) is at least 1 since this function is only called if there is a need_lock
		need_locks = [l for l in self.locks if l.stage == "need"]
		# Sort by time_ns. If multiple, the the one with the smaller id is first
		need_locks = sorted(need_locks, key=lambda l: int(l.id))
		need_locks = sorted(need_locks, key=lambda l: int(l.time_ns))
		return need_locks[0].id == need_lock.id


class AbstractLock:
	"""
		An abstract lock doesn't do anything by itself.
		A subclass should implement _lock and call _init_lock_file in its __init__.
	"""

	__slots__ = ("db_name", "need_lock", "has_lock", "snapshot")

	db_name: str
	need_lock: LockFileMeta
	has_lock: LockFileMeta
	snapshot: FileLocksSnapshot

	def __init__(self, db_name: str):
		self.db_name = db_name.replace("/", "___").replace(".", "____")

	def _init_lockfiles(self, mode: str):
		time_ns = time.monotonic_ns()
		t_id = str(threading.get_native_id())
		dir = os.path.join(config.storage_directory, ".ddb")

		self.need_lock = LockFileMeta(dir, self.db_name, t_id, time_ns, "need", mode)
		self.has_lock = LockFileMeta(dir, self.db_name, t_id, time_ns, "has", mode)

		if not os.path.isdir(dir):
			os.mkdir(dir)

	def _lock(self):
		raise NotImplementedError

	def _unlock(self):
		for p in ("need_lock", "has_lock"):
			try:
				if lock := getattr(self, p, None):
					os.unlink(lock.path)
			except FileNotFoundError:
				pass
			finally:
				setattr(self, p, None)

	def __enter__(self):
		self._lock()

	def __exit__(self, exc_type, exc_val, exc_tb):
		self._unlock()


class ReadLock(AbstractLock):
	def __init__(self, db_name: str):
		super().__init__(db_name)
		self._init_lockfiles("read")

	def _lock(self):
		# Instantly signal that we need to read
		os_touch(self.need_lock.path)
		self.snapshot = FileLocksSnapshot(self.need_lock)

		# Except if current thread already has a read lock
		if self.snapshot.exists(self.has_lock):
			os.unlink(self.need_lock.path)
			raise RuntimeError("Thread already has a read lock. Do not try to obtain a read lock twice.")

		# Iterate until this is the oldest need* lock and no haswrite locks exist, or no *write locks exist
		while True:
			# If no writing is happening, allow unlimited reading
			if not self.snapshot.any_write_locks:
				os_touch(self.has_lock.path)
				os.unlink(self.need_lock.path)
				return
			# A needwrite or haswrite lock exists
			if not self.snapshot.any_has_write_locks and self.snapshot.oldest_need(self.need_lock):
				os_touch(self.has_lock.path)
				os.unlink(self.need_lock.path)
				return
			time.sleep(SLEEP_TIMEOUT)
			self.snapshot = FileLocksSnapshot(self.need_lock)


class WriteLock(AbstractLock):
	def __init__(self, db_name: str):
		super().__init__(db_name)
		self._init_lockfiles("write")

	def _lock(self):
		# Instantly signal that we need to write
		os_touch(self.need_lock.path)
		self.snapshot = FileLocksSnapshot(self.need_lock)

		# Except if current thread already has a write lock
		if self.snapshot.exists(self.has_lock):
			os.unlink(self.need_lock.path)
			raise RuntimeError("Thread already has a write lock. Do try to obtain a write lock twice.")

		# Iterate until this is the oldest need* lock and no has* locks exist
		while True:
			if not self.snapshot.any_has_locks and self.snapshot.oldest_need(self.need_lock):
				os_touch(self.has_lock.path)
				os.unlink(self.need_lock.path)
				return
			time.sleep(SLEEP_TIMEOUT)
			self.snapshot = FileLocksSnapshot(self.need_lock)
