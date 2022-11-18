import zlib
import os
from . import config, utils



def read(db_name: str, *, start: int = None, end: int = None) -> bytes:
	"""
		Read the content of a file as bytes. Reading works even when the config
		changes, so a compressed ddb file can also be read if compression is
		disabled, and vice versa.

		If no compression is used, efficient reading can be done by specifying a start
		and end byte index, such that only the bytes in that range are read from the
		file.

		If compression is used, specifying a start and end byte index is still possible,
		but the entire file has to be read and decompressed first, and then the bytes
		in the range are returned. This is because the compressed file is not seekable.

		Args:
		- `db_name`: The name of the database file to read from.
		- `start`: The start byte index to read from.
		- `end`: The end byte index to read up to (not included).

		Raises:
		- `FileNotFoundError`: If the file does not exist as .json nor .ddb.
		- `OSError`: If no compression is used and `start` is negative.
		- `FileExistsError`: If the file exists as .json and .ddb.
	"""

	json_path, json_exists, ddb_path, ddb_exists = utils.file_info(db_name)

	if json_exists:
		if ddb_exists:
			raise FileExistsError(
				f"Inconsistent: \"{db_name}\" exists as .json and .ddb."
				"Please remove one of them."
			)
		with open(json_path, "rb") as f:
			if start is None and end is None:
				return f.read()
			start = start or 0
			f.seek(start)
			if end is None:
				return f.read()
			return f.read(end - start)
	if not ddb_exists:
		raise FileNotFoundError(f"No database file exists for \"{db_name}\"")
	with open(ddb_path, "rb") as f:
		json_bytes = zlib.decompress(f.read())
		if start is None and end is None:
			return json_bytes
		start = start or 0
		end = end or len(json_bytes)
		return json_bytes[start:end]




def write(db_name: str, dump: bytes, *, start: int = None):
	"""
		Write the bytes to the file of the db_path. If the db was compressed but no
		compression is enabled, remove the compressed file, and vice versa.

		Args:
		- `db_name`: The name of the database to write to.
		- `dump`: The bytes to write to the file, representing correct JSON when
		decoded.
		- `start`: The start byte index to write to. If None, the whole file is overwritten.
		If the original content was longer, the rest truncated.
	"""

	json_path, json_exists, ddb_path, ddb_exists = utils.file_info(db_name)

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
