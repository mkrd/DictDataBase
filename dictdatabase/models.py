from __future__ import annotations
from path_dict import PathDict
from . import utils, io_safe, writing


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
		return writing.DDBSession(self.db_name, as_PathDict=True)


	def read(self):
		"""
			Load the contents of the object from the database
		"""
		self.file_db = io_safe.read(self.db_name)
		if self.file_db is None or self.key not in self.file_db:
			return None
		self.data = self.file_db.get(self.key, None)
		return self
