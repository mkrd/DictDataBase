import zlib
import os
from . import config, utils



def read(db_name: str, start=None, end=None) -> bytes:
	"""
		Read the content of a file as bytes. Reading works even when the config
		changes, so a compressed ddb file can also be read if compression is
		disabled, and vice versa.

		Note: Only specify either both start and end, or none of them.

		Args:
		- `db_name`: The name of the database to read from.
		- `start`: The start index to read from.
		- `end`: The end index to read up to (not included).
	"""
	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)

	if json_exists:
		if ddb_exists:
			raise FileExistsError(f"Inconsistent: \"{db_name}\" exists as .json and .ddb")
		with open(json_path, "rb") as f:
			if start is None:
				return f.read()
			f.seek(start)
			return f.read(end - start)
	if not ddb_exists:
		raise FileNotFoundError(f"DB does not exist: \"{db_name}\"")
	with open(ddb_path, "rb") as f:
		json_bytes = zlib.decompress(f.read())
		if start is None:
			return json_bytes
		return json_bytes[start:end]



def write(db_name: str, dump: bytes, start=None):
	"""
		Write the bytes to the file of the db_path. If the db was compressed but
		now config.use_compression is False, remove the compressed file, and
		vice versa.

		Args:
		- `db_name`: The name of the database to write to.
		- `dump`: The bytes to write to the file, representing correct JSON when
		decoded.
	"""

	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)

	# Write bytes or string to file
	remove_file = None
	if config.use_compression:
		if start is not None:
			raise RuntimeError("Cannot write to compressed file at a specific index")
		write_file = ddb_path
		if json_exists:
			remove_file = json_path
		dump = zlib.compress(dump, 1)
	else:
		write_file = json_path
		if ddb_exists:
			remove_file = ddb_path

	# Write bytes or string to file
	if start is None:
		with open(write_file, "wb") as f:
			f.write(dump)
	else:
		with open(write_file, "ab") as f:
			f.seek(start)
			f.truncate()
			f.write(dump)

	# Remove the other file if it exists
	# This is done after writing to avoid data loss
	if remove_file is not None:
		os.remove(remove_file)
