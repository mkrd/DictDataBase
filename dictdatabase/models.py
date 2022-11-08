from __future__ import annotations
from typing import TypeVar, Any, Callable
from . import utils, io_safe, config
from . session import DDBSession

T = TypeVar("T")


class OperationType:
	"""
	Legal:
	- DDB.at("file")
	- DDB.at("file", key="subkey")
	- DDB.at("file", where=lambda k, v: ...)
	- DDB.at("dir", "*")
	- DDB.at("dir", "*", where=lambda k, v: ...)

	Illegal:
	- DDB.at("file", key="subkey", where=lambda k, v: ...)
	- DDB.at("dir", key="subkey", where=lambda k, v: ...)
	- DDB.at("dir", key="subkey")
	"""

	def __init__(self, path, key, where):
		self.dir = "*" in path
		self.file = not self.dir
		self.where = where is not None
		self.key = key is not None

		if self.key and self.where:
			raise TypeError("Cannot specify both key and where")
		if self.key and self.dir:
			raise TypeError("Cannot specify sub-key when selecting a folder. Specify the key in the path instead.")

	@property
	def file_normal(self):
		return self.file and not self.where and not self.key

	@property
	def file_key(self):
		return self.file and not self.where and self.key

	@property
	def file_where(self):
		return self.file and self.where and not self.key

	@property
	def dir_normal(self):
		return self.dir and not self.where and not self.key

	@property
	def dir_where(self):
		return self.dir and self.where and not self.key



def at(*path, key: str = None, where: Callable[[Any, Any], bool] = None) -> DDBMethodChooser:
	"""
	Select a file or folder to perform an operation on.
	If you want to select a specific key in a file, use the `key` parameter,
	e.g. `DDB.at("file", key="subkey")`.

	If you want to select an entire folder, use the `*` wildcard,
	eg. `DDB.at("folder", "*")`, or `DDB.at("folder/*")`. You can also use
	the `where` callback to select a subset of the file or folder.

	If the callback returns `True`, the item will be selected. The callback
	needs to accept a key and value as arguments.

	Args:
	- `path`: The path to the file or folder. Can be a string, a
	comma-separated list of strings, or a list.
	- `key`: The key to select from the file.
	- `where`: A function that takes a key and value and returns `True` if the
	key should be selected.

	Beware: If you select a folder with the `*` wildcard, you can't use the `key` parameter.
	Also, you cannot use the `key` and `where` parameters at the same time.
	"""
	return DDBMethodChooser(path, key, where)


class DDBMethodChooser:
	path: str
	key: str
	where: Callable[[Any, Any], bool]


	def __init__(self, path: tuple, key: str = None, where: Callable[[Any, Any], bool] = None):
		pc = []
		for p in path:
			pc += p if isinstance(p,  list) else [p]
		self.path = utils.to_path_str([str(p) for p in pc])
		self.key = key
		self.where = where
		self.op_type = OperationType(self.path, self.key, self.where)
		# Invariants:
		# - Both key and where cannot be not None at the same time
		# - If key is not None, then there is no wildcard in the path.


	def exists(self) -> bool:
		"""
		Efficiently checks if a database exists. If the selected path contains
		a wildcard, it will return True if at least one file exists in the folder.


		If a key was specified, check if it exists in a database.
		The key can be anywhere in the database, even deeply nested.
		As long it exists as a key in any dict, it will be found.
		"""
		if self.where is not None:
			raise RuntimeError("DDB.at(where=...).exists() cannot be used with the where parameter")

		occurs = len(utils.find(self.path)) > 0
		if not occurs:
			return False
		if self.key is None:
			return True
		# Key is passed and occurs is True
		try:
			return io_safe.partial_read(self.path, key=self.key) is not None
		except KeyError:
			return False


	def create(self, data=None, force_overwrite: bool = False):
		"""
		Create a new file with the given data as the content. If the file
		already exists, a FileExistsError will be raised unless
		`force_overwrite` is set to True.

		Args:
		- `data`: The data to write to the file. If not specified, an empty dict
		will be written.
		- `force_overwrite`: If `True`, will overwrite the file if it already
		exists, defaults to False (optional).
		"""
		if self.where is not None or self.key is not None:
			raise RuntimeError("DDB.at().create() cannot be used with the where or key parameters")

		# Except if db exists and force_overwrite is False
		if not force_overwrite and self.exists():
			raise FileExistsError(f"Database {self.path} already exists in {config.storage_directory}. Pass force_overwrite=True to overwrite.")
		# Write db to file
		if data is None:
			data = {}
		io_safe.write(self.path, data)


	def delete(self):
		"""
		Delete the file at the selected path.
		"""
		if self.where is not None or self.key is not None:
			raise RuntimeError("DDB.at().delete() cannot be used with the where or key parameters")
		io_safe.delete(self.path)


	def read(self, as_type: T = None) -> dict | T | None:
		"""
		Reads a file or folder depending on previous `.at(...)` selection.

		Args:
		- `as_type`: If provided, return the value as the given type.
		Eg. as_type=str will return str(value).
		"""

		def type_cast(value):
			if as_type is None:
				return value
			return as_type(value)

		data = {}

		if self.op_type.file_normal:
			data = io_safe.read(self.path)

		elif self.op_type.file_key:
			_, json_exists, _, ddb_exists = utils.db_paths(self.path)
			if not json_exists and not ddb_exists:
				return None
			data = io_safe.partial_read(self.path, self.key)

		elif self.op_type.file_where:
			file_content = io_safe.read(self.path)
			if file_content is None:
				return None
			for k, v in file_content.items():
				if self.where(k, type_cast(v)):
					data[k] = v

		elif self.op_type.dir_normal:
			pattern_paths = utils.expand_find_path_pattern(self.path)
			data = {n.split("/")[-1]: io_safe.read(n) for n in pattern_paths}

		elif self.op_type.dir_where:
			for db_name in utils.expand_find_path_pattern(self.path):
				k, v = db_name.split("/")[-1], io_safe.read(db_name)
				if self.where(k, type_cast(v)):
					data[k] = v

		return type_cast(data)


	def session(self, as_type: T = None) -> DDBSession[T]:
		"""
		Opens a session to the selected file(s) or folder, depending on previous
		`.at(...)` selection. Inside the with block, you have exclusive access
		to the file(s) or folder.
		Call `session.write()` to write the data to the file(s) or folder.

		Args:
		- `as_type`: If provided, cast the value to the given type.
		Eg. as_type=str will return str(value).
		"""
		return DDBSession(self.path, self.op_type, self.key, self.where, as_type)
