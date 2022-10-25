from __future__ import annotations
from typing import Any
from path_dict import PathDict
from path_dict import PathDict as pd
from . import utils, io_safe
from . sessions import DDBSession, DDBMultiSession, DDBSubSession


class SubModel(PathDict):
	def __init__(self, key: str, initial_value=None):
		"""
			Initialize with the initial_value dict or PathDict if it is given.
			If it is not given, it will be read from the db.
		"""
		self.key, self.db_name = key, utils.to_path_str(self.db_name)
		if initial_value is None:
			self.file_db = io_safe.read(self.db_name)
			if self.file_db is None:
				raise FileNotFoundError(f"DB {self.db_name} not found.")
			if self.key not in self.file_db:
				raise KeyError(f"DB {self.db_name} does not contain key {self.key}")
			super().__init__(self.file_db[self.key])
		elif isinstance(initial_value, (dict, PathDict)):
			super().__init__(initial_value)
		else:
			raise ValueError("If provided, initial_value must be a dict or PathDict")

	def session(self):
		return DDBSession(self.db_name, as_type=pd)

	def read(self):
		"""
			Load the contents of the object from the database
		"""
		self.file_db = io_safe.read(self.db_name)
		if self.file_db is None or self.key not in self.file_db:
			return None
		self.data = self.file_db.get(self.key, None)
		return self


def at(*path):
	return DDBMethodChooser(*path)


class DDBMethodChooser:
	def __init__(self, *path):
		if len(path) > 1:
			self.path = utils.to_path_str("/".join(path))
		else:
			self.path = utils.to_path_str(*path)

	def exists(self, key=None) -> bool:
		"""
			Efficiently checks if a database exists.
			If it contains a wildcard, it will return True if at least one exists.

			If the key is passed, check if it exists in a database.
			The key can be anywhere in the database, even deeply nested.
			As long it exists as a key in any dict, it will be found.
		"""
		occurs = len(utils.find(self.path)) > 0
		if not occurs:
			return False
		if key is None:
			return True
		# Key is passed and occurs is True
		try:
			io_safe.subread(self.path, key=key)
			return True
		except KeyError:
			return False

	def create(self, db=None, force_overwrite: bool = False):
		"""
		It creates a database file at the given path, and writes the given database to
		it

		:param db: The database to create. If not specified, an empty database is
		created.
		:param force_overwrite: If True, will overwrite the database if it already
		exists, defaults to False (optional).
		"""
		# Except if db exists and force_overwrite is False
		if not force_overwrite and self.exists():
			raise FileExistsError(f"Database {self.path} already exists. Pass force_overwrite=True to overwrite.")
		# Write db to file
		if db is None:
			db = {}
		data = db.dict if isinstance(db, PathDict) else db
		io_safe.write(self.path, data)

	def delete(self):
		"""
			Delete the database at the selected path.
		"""
		io_safe.delete(self.path)

	def read(self, key: str = None, as_type=None) -> dict | Any:
		"""
			Reads a database and returns it. If a key is given, return the value at that key, more info in Args.

			Args:
			- `key`: If provided, only return the value of the given key. The key
				can be anywhere in the database, even deeply nested. If multiple
				identical keys exist, the one at the outermost indentation will
				be returned. This is very fast, as it does not read the entire
				database, but only the key - value pair.
			- `as_type`: If provided, return the value as the given type. Eg. as=str will return str(value).
		"""
		if key is not None:
			if "*" in key:
				raise ValueError("A key cannot be specified with a wildcard.")
			# Subread
			_, json_exists, _, ddb_exists = utils.db_paths(self.path)
			if not json_exists and not ddb_exists:
				return None
			# Wait in any write lock case, "need" or "has".
			data = io_safe.partial_read(self.path, key)
		elif "*" in self.path:
			# Multiread
			pattern_paths = utils.expand_find_path_pattern(self.path)
			data = {db_name: io_safe.read(db_name) for db_name in pattern_paths}
		else:
			# Normal read
			data = io_safe.read(self.path)
		return as_type(data) if as_type is not None else data

	def session(self, key: str = None, as_type=None) -> DDBSession | DDBMultiSession | DDBSubSession:
		"""

			Open multiple files at once using a glob pattern, like "user/*".
			Mutliple arguments are allowed to access folders,
			so session(f"users/{user_id}") is equivalent
			to session("users", user_id).
		"""
		if key is not None and "*" in key:
			raise ValueError("A key cannot be specified with a wildcard.")
		if key is not None:
			return DDBSubSession(self.path, key, as_type)
		elif "*" in self.path:
			return DDBMultiSession(self.path, as_type)
		else:
			return DDBSession(self.path, as_type)
