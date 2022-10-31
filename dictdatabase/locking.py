from __future__ import annotations
import threading
import time
from pathlib import Path
import os
from . import config

SLEEP_TIMEOUT = 0.005

# If a process crashes and doesn't remove its locks, remove them after a timeout
LOCK_TIMEOUT = 30.0


def get_locks(db_name: str, *, id=None, time_ns=None, stage=None, mode=None):
	"""
		Get locks in the database directory.
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
		if time_ns is not None and f_time_ns != time_ns:
			continue
		if stage is not None and f_stage != stage:
			continue
		if mode is not None and f_mode != mode:
			continue
		res.append(x)
	return res


def remove_orphaned_locks(db_name, ignore=None):
	for lock in get_locks(db_name):
		if lock == ignore:
			continue
		_, _, time_ns, _, _, _ = lock.split(".")
		if time.monotonic_ns() - int(time_ns) > LOCK_TIMEOUT * 1_000_000_000:
			Path(lock).unlink()
			print(f"Found orphaned lock ({lock}). Remove")



def path_str(db_name, *, id, time_ns, stage, mode):
	l_name = f"{db_name}.{id}.{time_ns}.{stage}.{mode}.lock"
	l_path = os.path.join(config.storage_directory, ".ddb", l_name)
	Path(l_path).parent.mkdir(parents=True, exist_ok=True)
	return l_path


def is_oldest_need_lock(lock_id, db_name):
	need_locks = get_locks(db_name, stage="need")
	if len(need_locks) == 0:
		return True

	need_locks_id_time = []
	for lock in need_locks:
		_, l_id, l_time_ns, _, _, _ = lock.split(".")
		need_locks_id_time.append((l_id, l_time_ns))
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
	path: Path | None

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
		self.path.unlink()
		self.path = None

	def __enter__(self):
		self._lock()

	def __exit__(self, exc_type, exc_val, exc_tb):
		self._unlock()



class ReadLock(AbstractLock):
	def _lock(self):
		# Instantly signal that we need to read
		need_read_path_str = path_str(self.db_name, id=self.id, time_ns=self.time_ns, stage="need", mode="read")
		need_read_path = Path(need_read_path_str)
		need_read_path.touch()

		# Except if current thread already has a read lock
		if len(get_locks(self.db_name, id=self.id, stage="has", mode="read")) > 0:
			need_read_path.unlink()
			raise RuntimeError("Thread already has a read lock. Do not try to obtain a read lock twice.")

		# Make path of the hyptoetical hasread lock
		self.path = Path(path_str(self.db_name, id=self.id, time_ns=self.time_ns, stage="has", mode="read"))

		# Iterate until this is the oldest need* lock and no haswrite locks exist, or no *write locks exist
		while True:
			remove_orphaned_locks(self.db_name, ignore=need_read_path_str)
			# If no writing is happening, allow unlimited reading
			if len(get_locks(self.db_name, mode="write")) == 0:
				self.path.touch()
				need_read_path.unlink()
				return
			# A needwrite or haswrite lock exists
			if is_oldest_need_lock(self.id, self.db_name) and len(get_locks(self.db_name, stage="has", mode="write")) == 0:
				self.path.touch()
				need_read_path.unlink()
				return
			time.sleep(SLEEP_TIMEOUT)



class WriteLock(AbstractLock):
	def _lock(self):

		# Instantly signal that we need to write
		need_write_path_str = path_str(self.db_name, id=self.id, time_ns=self.time_ns, stage="need", mode="write")
		need_write_path = Path(need_write_path_str)
		need_write_path.touch()

		# Except if current thread already has a write lock
		if len(get_locks(self.db_name, id=self.id, stage="has", mode="write")) > 0:
			need_write_path.unlink()
			raise RuntimeError("Thread already has a write lock. Do try to obtain a write lock twice.")

		# Make path of the hyptoetical haswrite lock
		self.path = Path(path_str(self.db_name, id=self.id, time_ns=self.time_ns, stage="has", mode="write"))

		# Iterate until this is the oldest need* lock and no has* locks exist
		while True:
			remove_orphaned_locks(self.db_name, ignore=need_write_path_str)
			if is_oldest_need_lock(self.id, self.db_name) and len(get_locks(self.db_name, stage="has")) == 0:
				self.path.touch()
				need_write_path.unlink()
				return
			time.sleep(SLEEP_TIMEOUT)
