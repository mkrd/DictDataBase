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


def lock_file_exists(ddb_dir: str, db_name: str, id: str, stage: str, mode: str) -> bool:
	"""
		Like get_lock_file_names, but returns True if there is at least one
		lock file with the given arguments.
	"""
	for x in os.listdir(ddb_dir):
		if not x.endswith(".lock"):
			continue
		f_name, f_id, _, f_stage, f_mode, _ = x.split(".")
		if f_name != db_name or f_id != id or f_stage != stage or f_mode != mode:
			continue
		return True
	return False


class LockFileMeta:

	__slots__ = ("ddb_dir", "name", "id", "time_ns", "stage", "mode")

	ddb_dir: str
	name: str
	id: str
	time_ns: str
	stage: str
	mode: str

	def __init__(self, ddb_dir, f_name, f_id, f_time_ns, f_stage, f_mode):
		self.ddb_dir = ddb_dir
		self.name = f_name
		self.id = f_id
		self.time_ns = f_time_ns
		self.stage = f_stage
		self.mode = f_mode

	@property
	def path(self):
		lock_file = f"{self.name}.{self.id}.{self.time_ns}.{self.stage}.{self.mode}.lock"
		return os.path.join(self.ddb_dir, lock_file)


class FileLocksSnapshot:

	__slots__ = ("has_lock_count", "has_write_lock_count", "write_lock_count", "lock_files")

	def __init__(self, ddb_dir, db_name, ignore_during_orphan_check):
		self.has_lock_count = 0
		self.has_write_lock_count = 0
		self.write_lock_count = 0
		self.lock_files = []

		for x in os.listdir(ddb_dir):
			if not x.endswith(".lock"):
				continue
			f_name, f_id, f_time_ns, f_stage, f_mode, _ = x.split(".")
			if f_name != db_name:
				continue

			lock_meta = LockFileMeta(ddb_dir, f_name, f_id, f_time_ns, f_stage, f_mode)

			if lock_meta.path != ignore_during_orphan_check:
				lock_age = time.monotonic_ns() - int(lock_meta.time_ns)
				if lock_age > LOCK_TIMEOUT * 1_000_000_000:
					os.unlink(lock_meta.path)
					print(f"Found orphaned lock ({lock_meta.path}). Remove")
					continue

			if lock_meta.stage == "has":
				self.has_lock_count += 1
				if lock_meta.mode == "write":
					self.has_write_lock_count += 1
			if lock_meta.mode == "write":
				self.write_lock_count += 1

			self.lock_files.append(lock_meta)


	def get_need_locks(self) -> list[LockFileMeta]:
		return [l for l in self.lock_files if l.stage == "need"]


class AbstractLock:
	"""
		An abstract lock doesn't do anything by itself. A subclass of it needs to
		call super().__init__(...) and then only exit __init__ when the lock is aquired.
	"""

	__slots__ = ("id", "time_ns", "db_name", "need_path", "path", "ddb_dir", "locks_snapshot")

	id: str
	time_ns: int
	db_name: str
	need_path: str
	path: str
	ddb_dir: str
	locks_snapshot: FileLocksSnapshot

	def __init__(self, db_name: str):
		"""
			Create a lock, with the current thread id as the lock id,
			and the current time in nanoseconds as the time.
		"""
		self.id = str(threading.get_native_id())
		self.time_ns = time.monotonic_ns()
		self.db_name = db_name.replace("/", "___").replace(".", "____")
		self.ddb_dir = os.path.join(config.storage_directory, ".ddb")
		if not os.path.isdir(self.ddb_dir):
			os.mkdir(self.ddb_dir)

	def _lock(self):
		raise NotImplementedError

	def _unlock(self):
		for p in ("need_path", "path"):
			try:
				if path := getattr(self, p, None):
					os.unlink(path)
			except FileNotFoundError:
				pass
			finally:
				setattr(self, p, None)

	def __enter__(self):
		self._lock()

	def __exit__(self, exc_type, exc_val, exc_tb):
		self._unlock()

	def make_lock_path(self, stage: str, mode: str) -> str:
		return os.path.join(self.ddb_dir, f"{self.db_name}.{self.id}.{self.time_ns}.{stage}.{mode}.lock")

	def is_oldest_need_lock(self) -> bool:
		# len(need_locks) is at least 1 since this function is only called if there is a need_lock
		need_locks = self.locks_snapshot.get_need_locks()
		# Sort by time_ns. If multiple, the the one with the smaller id is first
		need_locks = sorted(need_locks, key=lambda l: int(l.id))
		need_locks = sorted(need_locks, key=lambda l: int(l.time_ns))
		return need_locks[0].id == self.id



class ReadLock(AbstractLock):
	def _lock(self):
		# Instantly signal that we need to read
		self.need_path = self.make_lock_path("need", "read")
		os_touch(self.need_path)

		# Except if current thread already has a read lock
		if lock_file_exists(self.ddb_dir, self.db_name, self.id, "has", "read"):
			os.unlink(self.need_path)
			raise RuntimeError("Thread already has a read lock. Do not try to obtain a read lock twice.")

		# Make path of the hyptoetical hasread lock
		self.path = self.make_lock_path("has", "read")

		# Iterate until this is the oldest need* lock and no haswrite locks exist, or no *write locks exist
		while True:
			self.locks_snapshot = FileLocksSnapshot(self.ddb_dir, self.db_name, self.need_path)
			# If no writing is happening, allow unlimited reading
			if self.locks_snapshot.write_lock_count == 0:
				os_touch(self.path)
				os.unlink(self.need_path)
				return
			# A needwrite or haswrite lock exists
			if self.is_oldest_need_lock() and self.locks_snapshot.has_write_lock_count == 0:
				os_touch(self.path)
				os.unlink(self.need_path)
				return
			time.sleep(SLEEP_TIMEOUT)



class WriteLock(AbstractLock):
	def _lock(self):
		# Instantly signal that we need to write
		self.need_path = self.make_lock_path("need", "write")
		os_touch(self.need_path)

		# Except if current thread already has a write lock
		if lock_file_exists(self.ddb_dir, self.db_name, self.id, "has", "write"):
			os.unlink(self.need_path)
			raise RuntimeError("Thread already has a write lock. Do try to obtain a write lock twice.")

		# Make path of the hyptoetical haswrite lock
		self.path = self.make_lock_path("has", "write")

		# Iterate until this is the oldest need* lock and no has* locks exist
		while True:
			self.locks_snapshot = FileLocksSnapshot(self.ddb_dir, self.db_name, self.need_path)
			if self.is_oldest_need_lock() and self.locks_snapshot.has_lock_count == 0:
				os_touch(self.path)
				os.unlink(self.need_path)
				return
			time.sleep(SLEEP_TIMEOUT)
