from __future__ import annotations

import contextlib
import os
import threading
import time

from . import config

# Design decisions:
# - Do not use pathlib, because it is slower than os

# Constants
SLEEP_TIMEOUT = 0.001 * 1  # (ms)
LOCK_KEEP_ALIVE_TIMEOUT = 0.001 * 0.08  # (ms)

# Duration to wait updating the timestamp of the lock file
ALIVE_LOCK_REFRESH_INTERVAL_NS = 1_000_000_000 * 10  # (s)

# Duration to wait before considering a lock as orphaned
REMOVE_ORPHAN_LOCK_TIMEOUT = 20.0

# Duration to wait before giving up on acquiring a lock
AQUIRE_LOCK_TIMEOUT = 60.0


def os_touch(path: str) -> None:
	"""
	Create an empty file at the given path. This mimics the UNIX touch command
	and is compatible with both Windows and UNIX systems.
	"""
	mode = 0o666
	flags = os.O_CREAT | os.O_WRONLY | os.O_EXCL
	fd = os.open(path, flags, mode)
	os.close(fd)


class LockFileMeta:
	"""
	Metadata representation for a lock file.
	"""

	__slots__ = ("ddb_dir", "name", "id", "time_ns", "stage", "mode", "path")

	ddb_dir: str
	name: str
	id: str
	time_ns: str
	stage: str
	mode: str
	path: str

	def __init__(self, ddb_dir: str, name: str, id: str, time_ns: str, stage: str, mode: str) -> None:
		self.ddb_dir = ddb_dir
		self.name = name
		self.id = id
		self.time_ns = time_ns
		self.stage = stage
		self.mode = mode
		lock_file = f"{name}.{id}.{time_ns}.{stage}.{mode}.lock"
		self.path = os.path.join(ddb_dir, lock_file)

	def __repr__(self) -> str:
		return f"LockFileMeta({self.ddb_dir=}, {self.name=}, {self.id=}, {self.time_ns=}, {self.stage=}, {self.mode=})"

	def new_with_updated_time(self) -> LockFileMeta:
		"""
		Create a new instance with an updated timestamp.
		"""
		time_ns = f"{time.time_ns()}"
		return LockFileMeta(self.ddb_dir, self.name, self.id, time_ns, self.stage, self.mode)


class FileLocksSnapshot:
	"""
	Represents a snapshot of the current state of file locks in the directory.
	This snapshot assists in deciding which lock should be acquired or released next.

	On init, orphaned locks are removed.
	"""

	__slots__ = ("any_has_locks", "any_write_locks", "any_has_write_locks", "locks")

	locks: list[LockFileMeta]
	any_has_locks: bool
	any_write_locks: bool
	any_has_write_locks: bool

	def __init__(self, need_lock: LockFileMeta) -> None:
		self.locks = []
		self.any_has_locks = False
		self.any_write_locks = False
		self.any_has_write_locks = False

		for file_name in os.listdir(need_lock.ddb_dir):
			if not file_name.endswith(".lock"):
				continue
			name, id, time_ns, stage, mode, _ = file_name.split(".")
			if name != need_lock.name:
				continue

			lock_meta = LockFileMeta(need_lock.ddb_dir, name, id, time_ns, stage, mode)

			# Remove orphaned locks
			if lock_meta.path != need_lock.path:
				lock_age = time.time_ns() - int(lock_meta.time_ns)
				if lock_age > REMOVE_ORPHAN_LOCK_TIMEOUT * 1_000_000_000:
					os.unlink(lock_meta.path)
					print(f"Removed orphaned lock ({lock_meta.path})")
					continue

			self.locks.append(lock_meta)

			# Update lock state flags
			if lock_meta.stage == "has":
				self.any_has_locks = True
				if lock_meta.mode == "write":
					self.any_has_write_locks = True
			if lock_meta.mode == "write":
				self.any_write_locks = True

	def exists(self, l: LockFileMeta) -> bool:
		"""
		Check if a lock with the same ID, stage, and mode exists in the current snapshot.
		"""
		return any(x.id == l.id and x.stage == l.stage and x.mode == l.mode for x in self.locks)

	def oldest_need(self, need_lock: LockFileMeta) -> bool:
		"""
		Determine if the provided 'need_lock' is the oldest among all 'need' locks in the snapshot.
		"""
		# len(need_locks) is at least 1 since this function is only called if there is a need_lock
		need_locks = [l for l in self.locks if l.stage == "need"]
		# Sort by time_ns. If multiple, the the one with the smaller id is first
		need_locks = sorted(need_locks, key=lambda l: (int(l.time_ns), int(l.id)))
		return need_locks[0].id == need_lock.id


class AbstractLock:
	"""
	Abstract base class for file locks. This class doesn't lock/unlock by itself but
	provides a blueprint for derived classes to implement.
	"""

	__slots__ = ("db_name", "need_lock", "has_lock", "snapshot", "mode", "is_alive" "keep_alive_thread")

	db_name: str
	need_lock: LockFileMeta
	has_lock: LockFileMeta
	snapshot: FileLocksSnapshot
	mode: str
	is_alive: bool
	keep_alive_thread: threading.Thread

	def __init__(self, db_name: str) -> None:
		# Normalize db_name to avoid file naming conflicts
		self.db_name = db_name.replace("/", "___").replace(".", "____")
		time_ns = time.time_ns()
		t_id = f"{threading.get_native_id()}"  # ID that's unique across processes and threads.
		dir = os.path.join(config.storage_directory, ".ddb")

		self.need_lock = LockFileMeta(dir, self.db_name, t_id, time_ns, "need", self.mode)
		self.has_lock = LockFileMeta(dir, self.db_name, t_id, time_ns, "has", self.mode)

		self.is_alive = False
		self.keep_alive_thread = None

		# Ensure lock directory exists
		if not os.path.isdir(dir):
			os.makedirs(dir, exist_ok=True)

	def _keep_alive_thread(self) -> None:
		"""
		Keep the lock alive by updating the timestamp of the lock file.
		"""

		current_has_lock_time_ns: int = int(self.has_lock.time_ns)

		while self.is_alive:
			time.sleep(LOCK_KEEP_ALIVE_TIMEOUT)
			if time.time_ns() - current_has_lock_time_ns < ALIVE_LOCK_REFRESH_INTERVAL_NS:
				continue

			# Assert: The lock is older than ALIVE_LOCK_REFRESH_INTERVAL_NS ns
			# This means the has_lock must be refreshed

			new_has_lock = self.has_lock.new_with_updated_time()
			os_touch(new_has_lock.path)
			with contextlib.suppress(FileNotFoundError):
				os.unlink(self.has_lock.path)  # Remove old lock file
			self.has_lock = new_has_lock
			current_has_lock_time_ns = int(new_has_lock.time_ns)

	def _start_keep_alive_thread(self) -> None:
		"""
		Start a thread that keeps the lock alive by updating the timestamp of the lock file.
		"""

		if self.keep_alive_thread is not None:
			raise RuntimeError("Keep alive thread already exists.")

		self.is_alive = True
		self.keep_alive_thread = threading.Thread(target=self._keep_alive_thread, daemon=False)
		self.keep_alive_thread.start()

	def _lock(self) -> None:
		"""Override this method to implement locking mechanism."""
		raise NotImplementedError

	def _unlock(self) -> None:
		"""Remove the lock files associated with this lock."""

		if self.keep_alive_thread is not None:
			self.is_alive = False
			self.keep_alive_thread.join()
			self.keep_alive_thread = None

		for p in ("need_lock", "has_lock"):
			try:
				if lock := getattr(self, p, None):
					os.unlink(lock.path)
			except FileNotFoundError:
				pass
			finally:
				setattr(self, p, None)

	def __enter__(self) -> None:
		self._lock()

	def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
		self._unlock()


class ReadLock(AbstractLock):
	"""
	A file-based read lock.
	Multiple threads/processes can simultaneously hold a read lock unless there's a write lock.
	"""

	mode = "read"

	def _lock(self) -> None:
		# Express intention to acquire read lock
		os.makedirs(os.path.dirname(self.need_lock.path), exist_ok=True)
		os_touch(self.need_lock.path)
		self.snapshot = FileLocksSnapshot(self.need_lock)

		# If this thread already holds a read lock, raise an exception.
		if self.snapshot.exists(self.has_lock):
			os.unlink(self.need_lock.path)
			raise RuntimeError("Thread already has a read lock. Do not try to obtain a read lock twice.")

		start_time = time.time()

		# Try to acquire lock until conditions are met or a timeout occurs
		while True:
			if not self.snapshot.any_write_locks or (
				not self.snapshot.any_has_write_locks and self.snapshot.oldest_need(self.need_lock)
			):
				self.has_lock = self.has_lock.new_with_updated_time()
				os_touch(self.has_lock.path)
				os.unlink(self.need_lock.path)
				self._start_keep_alive_thread()
				return
			time.sleep(SLEEP_TIMEOUT)
			if time.time() - start_time > AQUIRE_LOCK_TIMEOUT:
				raise RuntimeError("Timeout while waiting for read lock.")
			self.snapshot = FileLocksSnapshot(self.need_lock)


class WriteLock(AbstractLock):
	"""
	A file-based write lock.
	Only one thread/process can hold a write lock, blocking others from acquiring either read or write locks.
	"""

	mode = "write"

	def _lock(self) -> None:
		# Express intention to acquire write lock
		os.makedirs(os.path.dirname(self.need_lock.path), exist_ok=True)
		os_touch(self.need_lock.path)
		self.snapshot = FileLocksSnapshot(self.need_lock)

		# If this thread already holds a write lock, raise an exception.
		if self.snapshot.exists(self.has_lock):
			os.unlink(self.need_lock.path)
			raise RuntimeError("Thread already has a write lock. Do not try to obtain a write lock twice.")

		start_time = time.time()

		# Try to acquire lock until conditions are met or a timeout occurs
		while True:
			if not self.snapshot.any_has_locks and self.snapshot.oldest_need(self.need_lock):
				self.has_lock = self.has_lock.new_with_updated_time()
				os_touch(self.has_lock.path)
				os.unlink(self.need_lock.path)
				self._start_keep_alive_thread()
				return
			time.sleep(SLEEP_TIMEOUT)
			if time.time() - start_time > AQUIRE_LOCK_TIMEOUT:
				raise RuntimeError("Timeout while waiting for write lock.")
			self.snapshot = FileLocksSnapshot(self.need_lock)
