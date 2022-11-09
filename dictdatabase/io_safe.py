import os
from . import config, utils, io_unsafe, locking



def read(file_name: str) -> dict:
	"""
		Read the content of a file as a dict.

		Args:
		- `file_name`: The name of the file to read from.
	"""

	_, json_exists, _, ddb_exists = utils.file_info(file_name)

	if not json_exists and not ddb_exists:
		return None

	with locking.ReadLock(file_name):
		return io_unsafe.read(file_name)



def partial_read(file_name: str, key: str) -> dict:
	"""
		Read only the value of a key-value pair from a file.

		Args:
		- `file_name`: The name of the file to read from.
		- `key`: The key to read the value of.
	"""

	_, json_exists, _, ddb_exists = utils.file_info(file_name)

	if not json_exists and not ddb_exists:
		return None

	with locking.ReadLock(file_name):
		return io_unsafe.partial_read_only(file_name, key)



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

	json_path, json_exists, ddb_path, ddb_exists = utils.file_info(file_name)

	if not json_exists and not ddb_exists:
		return

	with locking.WriteLock(file_name):
		if json_exists:
			os.remove(json_path)
		if ddb_exists:
			os.remove(ddb_path)
