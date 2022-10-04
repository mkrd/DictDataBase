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

	if json_exists and ddb_exists:
		raise FileExistsError(f"DB Inconsistency: \"{db_name}\" exists as .json and .ddb")

	if not json_exists and not ddb_exists:
		raise FileNotFoundError(f"DB \"{db_name}\" does not exist.")


	data_str: str = None

	# Uncompressed json
	if json_exists:
		with open(json_path, "r") as f:
			data_str = f.read()
	# Compressed ddb
	elif ddb_exists:
		with open(ddb_path, "rb") as f:
			data_bytes = f.read()
			data_str = zlib.decompress(data_bytes).decode()

	# Use custom decoder if provided
	if config.custom_json_decoder is not None:
		return config.custom_json_decoder(data_str)
	# Otherwise, use default json decoder
	return json.loads(data_str)



def protected_read_json_as_dict(db_name: str):
	"""
		Ensure that reading only starts when there is no writing,
		and that while reading, no writing will happen.
		Otherwise, wait.
	"""

	json_path, json_exists, ddb_path, ddb_exists = db_paths(db_name)
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
	json_path, json_exists, ddb_path, ddb_exists = db_paths(db_name)

	# Dump db dict as string
	db_dump: str | bytes = None

	# Use custom encoder if provided
	if config.custom_json_encoder is not None and not config.use_compression:
		db_dump = config.custom_json_encoder(db)
	# Only generate pretty json if compression is disabled
	elif config.pretty_json_files and not config.use_compression:
		db_dump = json.dumps(db, indent="\t", sort_keys=True)
	# Generate compact json
	else:
		db_dump = json.dumps(db)

	# Compression is used
	if config.use_compression:
		if json_exists:
			os.remove(json_path)
		db_dump = zlib.compress(db_dump if isinstance(db_dump, bytes) else db_dump.encode(), 1)
		with open(ddb_path, "wb") as f:
			f.write(db_dump)
		return

	# No compression is used

	# Remove old ddb file if it exists
	# This is necessary when switching from compression to no compression
	if ddb_exists:
		os.remove(ddb_path)

	# If the custom encoder returns bytes, write them directly to the file
	if isinstance(db_dump, bytes):
		with open(json_path, "wb") as f:
			f.write(db_dump)
		return

	# Otherwise, write the string to the file
	with open(json_path, "w") as f:
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
