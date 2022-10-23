from __future__ import annotations
import glob
from . import utils, config

from path_dict import PathDict



def get_db_names(pattern: str):
	ddb_paths = glob.glob(f"{config.storage_directory}/{pattern}.ddb")
	json_paths = glob.glob(f"{config.storage_directory}/{pattern}.json")
	dbs_all = ddb_paths + json_paths
	dbs_all = [d.replace(f"{config.storage_directory}/", "") for d in dbs_all]
	return [d.replace(".ddb", "").replace(".json", "") for d in dbs_all]



class DDBSession(object):
	"""
		Enter:
		>>> with DDBSession(db_name) as session, data:

		Where `data` is the dict that was read from the filesystem. Modify
		`data` and call session.write() to save changes. If you don't call it,
		the changes will be lost after exiting the with statement.
	"""
	def __init__(self, db_name: str, as_PathDict: bool = False):
		self.db_name = db_name
		self.as_PathDict = as_PathDict
		self.in_session = False


	def __enter__(self):
		"""
			Any number of read tasks can be carried out in parallel.
			Each read task creates a read lock while reading, to signal that it is reading.

			As soon as a session starts, it writes a wants-to-write lock,
			No new read tasks will be allowed. When all read tasks are done, the session aquire the write lock.
			Now, it can savely read and write while all other tasks wait.
		"""
		self.write_lock = utils.WriteLock(self.db_name)
		self.in_session = True
		try:
			self.dict = utils.unprotected_read_json_as_dict(self.db_name)
			if self.as_PathDict:
				self.dict = PathDict(self.dict)
		except BaseException:
			self.write_lock.unlock()
			raise
		return self, self.dict


	def __exit__(self, type, value, tb):
		self.write_lock.unlock()
		self.write_lock = None
		self.in_session = False


	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")
		if self.as_PathDict:
			utils.unprotected_write_dict_as_json(self.db_name, self.dict.data)
		else:
			utils.unprotected_write_dict_as_json(self.db_name, self.dict)



class DDBMultiSession(object):
	def __init__(self, pattern: str, as_PathDict: bool = False):
		self.db_names = get_db_names(pattern)
		self.as_PathDict = as_PathDict
		self.in_session = False


	def __enter__(self):
		self.write_locks = [utils.WriteLock(x) for x in self.db_names]
		self.in_session = True
		try:
			self.dicts = {n: utils.unprotected_read_json_as_dict(n) for n in self.db_names}
			if self.as_PathDict:
				self.dicts = PathDict(self.dicts)
		except BaseException:
			for write_lock in self.write_locks:
				write_lock.unlock()
			raise
		return self, self.dicts


	def __exit__(self, type, value, tb):
		for write_lock in self.write_locks:
			write_lock.unlock()
		self.write_lock = None
		self.in_session = False


	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")
		for db_name in self.db_names:
			data = self.dicts[db_name].data if self.as_PathDict else self.dicts[db_name]
			utils.unprotected_write_dict_as_json(db_name, data)


def _to_path_if_tuple(s):
	if isinstance(s, tuple) and len(s) > 0:
		return "/".join(list(s))
	return s


class SubModel(PathDict):
	def __init__(self, key: str, initial_value=None):
		"""
			Initialize with the initial_value dict or PathDict if it is given.
			If it is not given, it will be read from the db.
		"""
		self.key, self.db_name = key, _to_path_if_tuple(self.db_name)
		if initial_value is None:
			self.file_db = utils.protected_read_json_as_dict(self.db_name)
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
		self.file_db = utils.protected_read_json_as_dict(self.db_name)
		if self.file_db is None or self.key not in self.file_db:
			return None
		self.data = self.file_db.get(self.key, None)
		return self
