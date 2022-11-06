from __future__ import annotations
import threading
import time
import os
from . import config

# Design decisions:
# - Do not use pathlib, because it is slower than os

SLEEP_TIMEOUT = 0.005

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


def get_lock_file_names(ddb_dir: str, db_name: str, *, id: str = None, time_ns: int = None, stage: str = None, mode: str = None) -> list[str]:
	"""
		Returns a list of lock file names in the configured storage directory as
		strings. The directory is not include, only the file names are returned.
		If any of the arguments are None, they are treated as a wildcard.

		Do not use glob, because it is slow. Compiling the regex pattern
		takes a long time, and it is called many times.
	"""
	res = []
	for x in os.listdir(ddb_dir):
		if not x.endswith(".lock"):
			continue
		f_name, f_id, f_time_ns, f_stage, f_mode, _ = x.split(".")
		if f_name != db_name:
			continue
		if id is not None and f_id != id:
			continue
		if time_ns is not None and f_time_ns != str(time_ns):
			continue
		if stage is not None and f_stage != stage:
			continue
		if mode is not None and f_mode != mode:
			continue
		res.append(x)
	return res


def any_lock_files(ddb_dir: str, db_name: str, *, id: str = None, time_ns: int = None, stage: str = None, mode: str = None) -> bool:
	"""
		Like get_lock_file_names, but returns True if there is at least one
		lock file with the given arguments.
	"""
	for x in os.listdir(ddb_dir):
		if not x.endswith(".lock"):
			continue
		f_name, f_id, f_time_ns, f_stage, f_mode, _ = x.split(".")
		if f_name != db_name:
			continue
		if id is not None and f_id != id:
			continue
		if time_ns is not None and f_time_ns != str(time_ns):
			continue
		if stage is not None and f_stage != stage:
			continue
		if mode is not None and f_mode != mode:
			continue
		return True
	return False


class AbstractLock:
	"""
		An abstract lock doesn't do anything by itself. A subclass of it needs to
		call super().__init__(...) and then only exit __init__ when the lock is aquired.
	"""
	id: str
	time_ns: int
	db_name: str
	need_path: str = None
	path: str = None

	def __init__(self, db_name: str):
		"""
			Create a lock, with the current thread id as the lock id,
			and the current time in nanoseconds as the time.
		"""
		self.id = str(threading.get_native_id())
		self.time_ns = time.monotonic_ns()
		self.db_name = db_name.replace("/", "___").replace(".", "____")
		self.ddb_dir = os.path.join(config.storage_directory, ".ddb")

	def _lock(self):
		raise NotImplementedError

	def _unlock(self):
		for p in ("need_path", "path"):
			try:
				path = getattr(self, p, None)
				if path:
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
		if not os.path.isdir(self.ddb_dir):
			os.mkdir(self.ddb_dir)
		return os.path.join(self.ddb_dir, f"{self.db_name}.{self.id}.{self.time_ns}.{stage}.{mode}.lock")

	def is_oldest_need_lock(self) -> bool:
		# len(need_locks) is at least 1 since this function is only called if
		# there is a need_lock
		need_locks = get_lock_file_names(self.ddb_dir, self.db_name, stage="need")
		# Get need locks id and time_ns
		need_locks_id_time = []
		for lock in need_locks:
			_, l_id, l_time_ns, _, _, _ = lock.split(".")
			need_locks_id_time.append((l_id, l_time_ns))
		# Sort by time_ns. If multiple, the the one with the smaller id is first
		need_locks_id_time = sorted(need_locks_id_time, key=lambda x: int(x[0]))  # Sort by id
		need_locks_id_time = sorted(need_locks_id_time, key=lambda x: int(x[1]))  # Sort by time_ns
		return need_locks_id_time[0][0] == self.id

	def remove_orphaned_locks(self):
		for lock_name in get_lock_file_names(self.ddb_dir, self.db_name):
			lock_path = os.path.join(self.ddb_dir, lock_name)
			if self.need_path == lock_path:
				continue
			_, _, time_ns, _, _, _ = lock_name.split(".")
			if time.monotonic_ns() - int(time_ns) > LOCK_TIMEOUT * 1_000_000_000:
				os.unlink(lock_path)
				print(f"Found orphaned lock ({lock_name}). Remove")




class ReadLock(AbstractLock):
	def _lock(self):
		# Instantly signal that we need to read
		self.need_path = self.make_lock_path("need", "read")
		os_touch(self.need_path)

		# Except if current thread already has a read lock
		if any_lock_files(self.ddb_dir, self.db_name, id=self.id, stage="has", mode="read"):
			os.unlink(self.need_path)
			raise RuntimeError("Thread already has a read lock. Do not try to obtain a read lock twice.")

		# Make path of the hyptoetical hasread lock
		self.path = self.make_lock_path("has", "read")

		# Iterate until this is the oldest need* lock and no haswrite locks exist, or no *write locks exist
		while True:
			self.remove_orphaned_locks()
			# If no writing is happening, allow unlimited reading
			if not any_lock_files(self.ddb_dir, self.db_name, mode="write"):
				os_touch(self.path)
				os.unlink(self.need_path)
				return
			# A needwrite or haswrite lock exists
			if self.is_oldest_need_lock() and not any_lock_files(self.ddb_dir, self.db_name, stage="has", mode="write"):
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
		if any_lock_files(self.ddb_dir, self.db_name, id=self.id, stage="has", mode="write"):
			os.unlink(self.need_path)
			raise RuntimeError("Thread already has a write lock. Do try to obtain a write lock twice.")

		# Make path of the hyptoetical haswrite lock
		self.path = self.make_lock_path("has", "write")

		# Iterate until this is the oldest need* lock and no has* locks exist
		while True:
			self.remove_orphaned_locks()
			if self.is_oldest_need_lock() and not any_lock_files(self.ddb_dir, self.db_name, stage="has"):
				os_touch(self.path)
				os.unlink(self.need_path)
				return
			time.sleep(SLEEP_TIMEOUT)
