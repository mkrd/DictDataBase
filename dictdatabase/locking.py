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


def get_lock_file_names(db_name: str, *, id: str = None, time_ns: int = None, stage: str = None, mode: str = None) -> list[str]:
	"""
		Returns a list of lock file names in the configured storage directory as
		strings. The directory is not include, only the file names are returned.
		If any of the arguments are None, they are treated as a wildcard.

		Do not use glob, because it is slow. Compiling the regex pattern
		takes a long time, and it is called many times.
	"""
	res = []
	for x in os.listdir(os.path.join(config.storage_directory, ".ddb")):
		if not x.startswith(db_name) or not x.endswith(".lock"):
			continue
		_, f_id, f_time_ns, f_stage, f_mode, _ = x.split(".")
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


def count_lock_files(db_name: str, *, id: str = None, time_ns: int = None, stage: str = None, mode: str = None) -> int:
	"""
		Like get_lock_file_names, but returns the number of lock files.
	"""
	res = 0
	for x in os.listdir(os.path.join(config.storage_directory, ".ddb")):
		if not x.startswith(db_name) or not x.endswith(".lock"):
			continue
		_, f_id, f_time_ns, f_stage, f_mode, _ = x.split(".")
		if id is not None and f_id != id:
			continue
		if time_ns is not None and f_time_ns != str(time_ns):
			continue
		if stage is not None and f_stage != stage:
			continue
		if mode is not None and f_mode != mode:
			continue
		res += 1
	return res


def make_lock_path(db_name: str, id: str, time_ns: int, stage: str, mode: str) -> str:
	ddb_dir_path = os.path.join(config.storage_directory, ".ddb")
	if not os.path.isdir(ddb_dir_path):
		os.mkdir(ddb_dir_path)
	return os.path.join(ddb_dir_path, f"{db_name}.{id}.{time_ns}.{stage}.{mode}.lock")


def remove_orphaned_locks(db_name: str, ignore: str = None):
	ddb_dir = os.path.join(config.storage_directory, ".ddb")
	for lock_name in get_lock_file_names(db_name):
		lock_path = os.path.join(ddb_dir, lock_name)
		if ignore == lock_path:
			continue
		_, _, time_ns, _, _, _ = lock_name.split(".")
		if time.monotonic_ns() - int(time_ns) > LOCK_TIMEOUT * 1_000_000_000:
			os.unlink(lock_path)
			print(f"Found orphaned lock ({lock_name}). Remove")


def is_oldest_need_lock(lock_id: str, db_name: str):
	# len(need_locks) is at least 1 since this function is only called if
	# there is a need_lock
	need_locks = get_lock_file_names(db_name, stage="need")
	# Get need locks id and time_ns
	need_locks_id_time = []
	for lock in need_locks:
		_, l_id, l_time_ns, _, _, _ = lock.split(".")
		need_locks_id_time.append((l_id, l_time_ns))
	# Sort by time_ns. If multiple, the the one with the smaller id is first
	need_locks_id_time = sorted(need_locks_id_time, key=lambda x: int(x[0]))  # Sort by id
	need_locks_id_time = sorted(need_locks_id_time, key=lambda x: int(x[1]))  # Sort by time_ns
	return need_locks_id_time[0][0] == lock_id


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


class ReadLock(AbstractLock):
	def _lock(self):
		# Instantly signal that we need to read
		self.need_path = make_lock_path(self.db_name, self.id, self.time_ns, "need", "read")
		os_touch(self.need_path)


		# Except if current thread already has a read lock
		if count_lock_files(self.db_name, id=self.id, stage="has", mode="read") > 0:
			os.unlink(self.need_path)
			raise RuntimeError("Thread already has a read lock. Do not try to obtain a read lock twice.")

		# Make path of the hyptoetical hasread lock
		self.path = make_lock_path(self.db_name, self.id, self.time_ns, "has", "read")

		# Iterate until this is the oldest need* lock and no haswrite locks exist, or no *write locks exist
		while True:
			remove_orphaned_locks(self.db_name, ignore=self.need_path)
			# If no writing is happening, allow unlimited reading
			if count_lock_files(self.db_name, mode="write") == 0:
				os_touch(self.path)
				os.unlink(self.need_path)
				return
			# A needwrite or haswrite lock exists
			if is_oldest_need_lock(self.id, self.db_name) and count_lock_files(self.db_name, stage="has", mode="write") == 0:
				os_touch(self.path)
				os.unlink(self.need_path)
				return
			time.sleep(SLEEP_TIMEOUT)



class WriteLock(AbstractLock):
	def _lock(self):
		# Instantly signal that we need to write
		self.need_path = make_lock_path(self.db_name, self.id, self.time_ns, "need", "write")
		os_touch(self.need_path)

		# Except if current thread already has a write lock
		if count_lock_files(self.db_name, id=self.id, stage="has", mode="write") > 0:
			os.unlink(self.need_path)
			raise RuntimeError("Thread already has a write lock. Do try to obtain a write lock twice.")

		# Make path of the hyptoetical haswrite lock
		self.path = make_lock_path(self.db_name, self.id, self.time_ns, "has", "write")

		# Iterate until this is the oldest need* lock and no has* locks exist
		while True:
			remove_orphaned_locks(self.db_name, ignore=self.need_path)
			if is_oldest_need_lock(self.id, self.db_name) and count_lock_files(self.db_name, stage="has") == 0:
				os_touch(self.path)
				os.unlink(self.need_path)
				return
			time.sleep(SLEEP_TIMEOUT)
