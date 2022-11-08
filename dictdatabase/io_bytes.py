import zlib
import os
from . import config, utils


def read(db_name: str, start=None, end=None) -> bytes:
	"""
		Read the content of a db as as bytes. Reading works even when the config
		changes, so a compressed ddb file can also be read if compression is
		disabled, and vice versa.
	"""
	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)

	if json_exists:
		if ddb_exists:
			raise FileExistsError(f"DB Inconsistency: \"{db_name}\" exists as .json and .ddb")
		with open(json_path, "rb") as f:
			if start is not None and end is not None:
				f.seek(start)
				return f.read(end - start)
			return f.read()
	if not ddb_exists:
		raise FileNotFoundError(f"DB does not exist: \"{db_name}\"")
	with open(ddb_path, "rb") as f:
		json_bytes = zlib.decompress(f.read())
		if start is not None and end is not None:
			return json_bytes[start:end]
		return json_bytes


def write(db_name: str, dump: bytes):
	"""
		Write the bytes to the file of the db_path.
		If the db was compressed but now config.use_compression is False,
		remove the compressed file, and vice versa.
	"""
	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)
	# Write bytes or string to file
	remove_this = None
	if config.use_compression:
		write_path = ddb_path
		if json_exists:
			remove_this = json_path
		dump = zlib.compress(dump, 1)
	else:
		write_path = json_path
		if ddb_exists:
			remove_this = ddb_path

	# Write bytes or string to file
	with open(write_path, "wb") as f:
		f.write(dump)

	# Remove the other file if it exists
	# This is done after writing to avoid data loss
	if remove_this is not None:
		os.remove(remove_this)
