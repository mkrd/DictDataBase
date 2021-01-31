import time
import string
from pathlib import Path
import glob
import random

from . import config

SLEEP_TIMEOUT = 0.01

# If a process crashes and doesn't clean its locks, remove them after a timeout
LOCK_TIMEOUT = 40.0



def clean_dead_locks(db_name, ignore=None):
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
		db_name[-1] = "." + db_name[-1]
		db_name = "/".join(db_name)
	else:
		path += "."
	return f"{path}{db_name}.{lock_id}.{time_ns}.{lock_type}.lock"



def find_locks(lock_type: str, db_name: str):
	return glob.glob(path_str(db_name, "*", "*", lock_type))



def is_oldest_lock_candidate(lock_id, db_name):
	write_candidates = find_locks("needwrite", db_name)
	write_candidates = [x.split(".")[:-2][-2:] for x in write_candidates]
	oldest_candidate = min(write_candidates, key=lambda x: int(x[1]))[0]
	return oldest_candidate == lock_id



class AbstractLock(object):
	"""
		An abstract lock doesn't do anything by itself. A subclass of it needs to
		call super().__init__(...) and then only exit when the lock is aquired.
	"""
	def __init__(self, db_name):
		"""
			If key is None, create a random identifier.
			if time_ns is None, initialize it to the current time.
		"""
		# Create a random id (62^5 = 916.132.832 possibilities)
		self.id = "".join(random.choices(string.ascii_letters + string.digits, k=5))
		self.time_ns = time.time_ns()
		self.db_name = db_name
		self.path = None

	def unlock(self):
		self.path.unlink()
		self.path = None



class ReadLock(AbstractLock):
	def __init__(self, db_name):
		super().__init__(db_name)
		has_read_path_str = path_str(db_name, self.id, self.time_ns, "hasread")
		self.path = Path(has_read_path_str)
		while True:
			clean_dead_locks(db_name, ignore=has_read_path_str)
			if len(find_locks("*write", db_name)) == 0:
				self.path.touch()
				return
			time.sleep(SLEEP_TIMEOUT)



class WriteLock(AbstractLock):
	def __init__(self, db_name):
		super().__init__(db_name)
		need_write_path_str = path_str(db_name, self.id, self.time_ns, "needwrite")
		need_write_path = Path(need_write_path_str)
		need_write_path.touch()
		self.path = Path(path_str(db_name, self.id, self.time_ns, "haswrite"))
		while True:
			clean_dead_locks(db_name, ignore=need_write_path_str)
			if is_oldest_lock_candidate(self.id, db_name) and len(find_locks("has*", db_name)) == 0:
				self.path.touch()
				need_write_path.unlink()
				return
			time.sleep(SLEEP_TIMEOUT)