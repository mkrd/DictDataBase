from __future__ import annotations
from dataclasses import dataclass
from path_dict import PathDict
from . import utils, io_safe, reading
from . writing import DDBSession, DDBMultiSession, DDBSubSession, create


@dataclass(frozen=True)
class PartialFileHandle:
	db_name: str
	key: str
	key_value: dict
	value_start_index: int
	value_end_index: int
	original_data_str: str
	indent_level: int
	indent_char: str


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
		return DDBSession(self.db_name, as_PathDict=True)


	def read(self):
		"""
			Load the contents of the object from the database
		"""
		self.file_db = io_safe.read(self.db_name)
		if self.file_db is None or self.key not in self.file_db:
			return None
		self.data = self.file_db.get(self.key, None)
		return self



class DDBMethodChooser:
	def __init__(self, *path):
		self.path = utils.to_path_str(*path)

	def exists(self) -> bool:
		"""
			Efficiently checks if a database exists.
			If it contains a wildcard, it will return True if at least one exists.
		"""
		return len(utils.find(self.path)) > 0

	def haskey(self, key: str) -> bool:
		return reading.haskey(self.path, key)

	def create(self, db=None, force_overwrite=False):
		create(db, force_overwrite)

	def delete(self):
		io_safe.delete(self.path)

	def read(self, key: str = None, as_PathDict: bool = False) -> dict | PathDict:
		"""
			Reads a database and returns it as a PathDict.
			If a key is given, return the efficiently read key value.
		"""
		if key is not None and "*" in key:
			raise ValueError("A key cannot be specified with a wildcard.")
		if key is not None:
			return reading.subread(self.path, key, as_PathDict=as_PathDict)
		elif "*" in self.path:
			return reading.multiread(self.path, as_PathDict=as_PathDict)
		else:
			return reading.read(self.path, as_PathDict=as_PathDict)

	def session(self, key: str = None, as_PathDict: bool = False) -> DDBSession | DDBMultiSession | DDBSubSession:
		if key is not None and "*" in key:
			raise ValueError("A key cannot be specified with a wildcard.")
		if key is not None:
			return DDBSubSession(self.path, key, as_PathDict=as_PathDict)
		elif "*" in self.path:
			return DDBMultiSession(self.path, as_PathDict=as_PathDict)
		else:
			return DDBSession(self.path, as_PathDict=as_PathDict)
