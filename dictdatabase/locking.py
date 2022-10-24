from __future__ import annotations
import threading
import time
from pathlib import Path
import glob
from . import config

SLEEP_TIMEOUT = 0.01

# If a process crashes and doesn't remove its locks, remove them after a timeout
LOCK_TIMEOUT = 30.0


def remove_dead_locks(db_name, ignore=None):
	db_locks = glob.glob(path_str(db_name, "*", "*", "*"))
	for lock in db_locks:
		if lock == ignore:
			continue
		lock_parts = lock.split(".")
		time_ns = lock_parts[-3]
		if time.time_ns() - int(time_ns) > LOCK_TIMEOUT * 1_000_000_000:
			Path(lock).unlink()
			print(f"Found dead lock ({lock}). Remove")


def path_str(db_name, lock_id, time_ns, lock_type):
	path = f"{config.storage_directory}/"
	if "/" in db_name:
		db_name = db_name.split("/")
		db_name[-1] = f".{db_name[-1]}"
		db_name = "/".join(db_name)
	else:
		path += "."
	return f"{path}{db_name}.{lock_id}.{time_ns}.{lock_type}.lock"


def check_if_lock_exists(db_name: str, thread_id: str, lock_type: str):
	locks = glob.glob(path_str(db_name, thread_id, "*", lock_type))
	return len(locks) > 0


def find_locks(lock_type: str, db_name: str):
	return glob.glob(path_str(db_name, "*", "*", lock_type))


def is_oldest_need_lock(lock_id, db_name):
	lock_candidates = find_locks("need*", db_name)
	lock_candidates = [x.split(".")[:-2][-2:] for x in lock_candidates]
	oldest_candidate = min(lock_candidates, key=lambda x: int(x[1]))[0]
	return oldest_candidate == lock_id



class AbstractLock(object):
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
		self.time_ns = time.time_ns()
		self.db_name = db_name
		self.path = None

	def unlock(self):
		self.path.unlink()
		self.path = None



class ReadLock(AbstractLock):
	def __init__(self, db_name):
		super().__init__(db_name)

		# Instantly signal that we need to read
		need_read_path_str = path_str(db_name, self.id, self.time_ns, "needread")
		need_read_path = Path(need_read_path_str)
		need_read_path.touch()

		# Except if current thread already has a read lock
		if check_if_lock_exists(db_name, self.id, "hasread"):
			need_read_path.unlink()
			raise RuntimeError("Thread already has a read lock. Do not try to obtain a read lock twice.")

		# Make path of the hyptoetical hasread lock
		self.path = Path(path_str(db_name, self.id, self.time_ns, "hasread"))

		# Iterate until this is the oldest need* lock and no haswrite locks exist, or no *write locks exist
		while True:
			remove_dead_locks(db_name, ignore=need_read_path_str)
			# If no writing is happening, allow unlimited reading
			if len(find_locks("*write", db_name)) == 0:
				self.path.touch()
				need_read_path.unlink()
				return
			# A needwrite or haswrite lock exists
			if is_oldest_need_lock(self.id, db_name) and len(find_locks("haswrite", db_name)) == 0:
				self.path.touch()
				need_read_path.unlink()
				return
			time.sleep(SLEEP_TIMEOUT)



class WriteLock(AbstractLock):
	def __init__(self, db_name):
		super().__init__(db_name)

		# Instantly signal that we need to write
		need_write_path_str = path_str(db_name, self.id, self.time_ns, "needwrite")
		need_write_path = Path(need_write_path_str)
		need_write_path.touch()

		# Except if current thread already has a write lock
		if check_if_lock_exists(db_name, self.id, "haswrite"):
			need_write_path.unlink()
			raise RuntimeError("Thread already has a write lock. Do try to obtain a write lock twice.")

		# Make path of the hyptoetical haswrite lock
		self.path = Path(path_str(db_name, self.id, self.time_ns, "haswrite"))

		# Iterate until this is the oldest need* lock and no has* locks exist
		while True:
			remove_dead_locks(db_name, ignore=need_write_path_str)
			if is_oldest_need_lock(self.id, db_name) and len(find_locks("has*", db_name)) == 0:
				self.path.touch()
				need_write_path.unlink()
				return
			time.sleep(SLEEP_TIMEOUT)
