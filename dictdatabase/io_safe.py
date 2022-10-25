import os
from . import config, utils, io_unsafe, locking


def read(db_name: str):
	"""
		Ensure that reading only starts when there is no writing,
		and that while reading, no writing will happen.
		Otherwise, wait.
	"""

	_, json_exists, _, ddb_exists = utils.db_paths(db_name)
	if not json_exists and not ddb_exists:
		return None
	# Wait in any write lock case, "need" or "has".
	lock = locking.ReadLock(db_name)
	try:
		return io_unsafe.read(db_name)
	except BaseException as e:
		raise e
	finally:
		lock.unlock()


def partial_read(db_name: str, key: str):
	_, json_exists, _, ddb_exists = utils.db_paths(db_name)
	if not json_exists and not ddb_exists:
		return None
	# Wait in any write lock case, "need" or "has".
	lock = locking.ReadLock(db_name)
	try:
		return io_unsafe.partial_read(db_name, key).key_value
	except BaseException as e:
		raise e
	finally:
		lock.unlock()


def write(db_name: str, db: dict):
	"""
		Ensures that writing only starts if there is no reading or writing in progress.
	"""
	dirname = os.path.dirname(f"{config.storage_directory}/{db_name}.any")
	os.makedirs(dirname, exist_ok=True)
	write_lock = locking.WriteLock(db_name)
	try:
		io_unsafe.write(db_name, db)
	except BaseException as e:
		raise e
	finally:
		write_lock.unlock()


def delete(db_name: str):
	"""
		Ensures that deleting only starts if there is no reading or writing in progress.
	"""
	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)
	if not json_exists and not ddb_exists:
		return None
	write_lock = locking.WriteLock(db_name)
	try:
		if json_exists:
			os.remove(json_path)
		if ddb_exists:
			os.remove(ddb_path)
	except BaseException as e:
		raise e
	finally:
		write_lock.unlock()
