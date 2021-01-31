import os
import json
import zlib
from typing import Tuple

from .locking import ReadLock, WriteLock
from . import config



def db_paths(db_name: str) -> Tuple[str, bool, str, bool]:
	base = f"{config.storage_directory}/{db_name}"
	j, d = f"{base}.json", f"{base}.ddb"
	return j, os.path.exists(j), d, os.path.exists(d)



def unprotected_read_json_as_dict(db_name: str) -> dict:
	"""
		Read the file at db_path from the configured storage directory.
		Make sure the file exists!
	"""
	json_path, json_exists, ddb_path, ddb_exists = db_paths(db_name)


	read_mode, db_path = None, None
	if json_exists and ddb_exists:
		raise Exception(f"DB Inconsistency: \"{db_name}\" exists as .json and .ddb")
	elif json_exists:
		read_mode, db_path = "r", json_path
	elif ddb_exists:
		read_mode, db_path = "rb", ddb_path
	else:
		raise Exception(f"DB \"{db_name}\" does not exist.")

	with open(db_path, read_mode) as f:
		f_str = f.read()
		if read_mode == "rb":
			f_str = zlib.decompress(f_str).decode()
		return json.loads(f_str)



def protected_read_json_as_dict(db_name: str):
	"""
		Ensure that reading only starts when there is no writing,
		and that while reading, no writing will happen.
		Otherwise, wait.
	"""

	_, json_exists, _, ddb_exists = db_paths(db_name)
	if not json_exists and not ddb_exists:
		return None
	# Wait in any write lock case, "need" or "has".
	lock = ReadLock(db_name)
	res = unprotected_read_json_as_dict(db_name)
	lock.unlock()
	return res





def unprotected_write_dict_as_json(db_name: str, db: dict):
	"""
		Write the dict db dumped as a json string
		to the file of the db_path.
	"""
	db_dump = None
	if config.pretty_json_files and not config.use_compression:
		db_dump = json.dumps(db, indent="\t", sort_keys=True)
	else:
		db_dump = json.dumps(db)

	json_path, json_exists, ddb_path, ddb_exists = db_paths(db_name)

	write_mode, db_path = None, None
	if config.use_compression:
		write_mode, db_path = "wb", ddb_path
		if json_exists:
			os.remove(json_path)
	else:
		write_mode, db_path = "w+", json_path
		if ddb_exists:
			os.remove(ddb_path)

	with open(db_path, write_mode) as f:
		if config.use_compression:
			db_dump = zlib.compress(db_dump.encode(), 1)
		f.write(db_dump)



def protected_write_dict_as_json(db_name: str, db: dict):
	"""
		Ensures that writing only starts if there is no reading or writing in progress.
	"""
	dirname = os.path.dirname(f"{config.storage_directory}/{db_name}.any")
	os.makedirs(dirname, exist_ok=True)

	write_lock = WriteLock(db_name)
	unprotected_write_dict_as_json(db_name, db)
	write_lock.unlock()


def protected_delete(db_name: str):
	"""
		Ensures that deleting only starts if there is no reading or writing in progress.
	"""
	json_path, json_exists, ddb_path, ddb_exists = db_paths(db_name)
	if not json_exists and not ddb_exists:
		return None
	write_lock = WriteLock(db_name)
	if json_exists:
		os.remove(json_path)
	if ddb_exists:
		os.remove(ddb_path)
	write_lock.unlock()