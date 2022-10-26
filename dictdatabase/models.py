from __future__ import annotations
from typing import TypeVar
from . import utils, io_safe
from . session import DDBSession

T = TypeVar("T")


def at(*path) -> DDBMethodChooser:
	return DDBMethodChooser(path)


class DDBMethodChooser:
	def __init__(self, path: tuple):
		self.path = utils.to_path_str("/".join(path))


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
		io_safe.write(self.path, db)

	def delete(self):
		"""
			Delete the database at the selected path.
		"""
		io_safe.delete(self.path)

	def read(self, key: str = None, as_type: T = None) -> dict | T:
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

	def session(self, key: str = None, as_type: T = None) -> DDBSession[T]:
		"""
			Open multiple files at once using a glob pattern, like "user/*".
			Mutliple arguments are allowed to access folders,
			so session(f"users/{user_id}") is equivalent
			to session("users", user_id).
		"""
		return DDBSession(self.path, key, as_type)
