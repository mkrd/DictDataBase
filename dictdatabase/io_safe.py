import os
from . import config, utils, io_unsafe, locking

from .file_meta import DBFileMeta



def read(file_name: str) -> dict:
	"""
		Read the content of a file as a dict.

		Args:
		- `file_name`: The name of the file to read from.
	"""

	file_meta = DBFileMeta(file_name)
	if not file_meta.exists:
		return None

	with locking.ReadLock(file_meta.path):
		return io_unsafe.read(file_meta)



def partial_read(file_name: str, key: str) -> dict:
	"""
		Read only the value of a key-value pair from a file.

		Args:
		- `file_name`: The name of the file to read from.
		- `key`: The key to read the value of.
	"""

	file_meta = DBFileMeta(file_name)
	if not file_meta.exists:
		return None

	with locking.ReadLock(file_name):
		return io_unsafe.partial_read_only(file_meta, key)



def write(file_name: str, data: dict):
	"""
		Ensures that writing only starts if there is no reading or writing in progress.

		Args:
		- `file_name`: The name of the file to write to.
		- `data`: The data to write to the file.
	"""

	dirname = os.path.dirname(f"{config.storage_directory}/{file_name}.any")
	os.makedirs(dirname, exist_ok=True)

	with locking.WriteLock(file_name):
		io_unsafe.write(file_name, data)



def delete(file_name: str):
	"""
		Ensures that deleting only starts if there is no reading or writing in progress.

		Args:
		- `file_name`: The name of the file to delete.
	"""

	file_meta = DBFileMeta(file_name)
	if not file_meta.exists:
		return None

	with locking.WriteLock(file_name):
		if file_meta.json_exists:
			os.remove(file_meta.json_path)
		if file_meta.ddb_exists:
			os.remove(file_meta.ddb_path)
