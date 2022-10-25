from __future__ import annotations
from typing import Tuple
from path_dict import PathDict
from . import utils, io_unsafe, locking


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

	def __enter__(self) -> Tuple("DDBSession", dict | PathDict):
		"""
			Any number of read tasks can be carried out in parallel.
			Each read task creates a read lock while reading, to signal that it is reading.

			As soon as a session starts, it writes a wants-to-write lock,
			No new read tasks will be allowed. When all read tasks are done, the session aquire the write lock.
			Now, it can savely read and write while all other tasks wait.
		"""
		self.write_lock = locking.WriteLock(self.db_name)
		self.in_session = True
		try:
			self.dict = io_unsafe.read(self.db_name)
			if self.as_PathDict:
				self.dict = PathDict(self.dict)
		except BaseException as e:
			self.write_lock.unlock()
			raise e
		return self, self.dict

	def __exit__(self, type, value, tb):
		self.write_lock.unlock()
		self.write_lock = None
		self.in_session = False

	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")
		data = self.dict.data if self.as_PathDict else self.dict
		io_unsafe.write(self.db_name, data)



class DDBMultiSession(object):
	def __init__(self, pattern: str, as_PathDict: bool = False):
		self.db_names = utils.find(pattern)
		self.as_PathDict = as_PathDict
		self.in_session = False

	def __enter__(self) -> Tuple("DDBMultiSession", dict | PathDict):
		self.write_locks = [locking.WriteLock(x) for x in self.db_names]
		self.in_session = True
		try:
			self.dicts = {n: io_unsafe.read(n) for n in self.db_names}
			if self.as_PathDict:
				self.dicts = PathDict(self.dicts)
		except BaseException as e:
			for write_lock in self.write_locks:
				write_lock.unlock()
			raise e
		return self, self.dicts

	def __exit__(self, type, value, tb):
		for write_lock in self.write_locks:
			write_lock.unlock()
		self.write_locks = None
		self.in_session = False

	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")
		for db_name in self.db_names:
			data = self.dicts[db_name].data if self.as_PathDict else self.dicts[db_name]
			io_unsafe.write(db_name, data)


class DDBSubSession(object):
	def __init__(self, db_name: str, key: str, as_PathDict: bool = False):
		self.db_name = db_name
		self.key = key
		self.as_PathDict = as_PathDict
		self.in_session = False

	def __enter__(self) -> Tuple("DDBSubSession", dict | PathDict):
		self.write_lock = locking.WriteLock(self.db_name)
		self.in_session = True
		try:
			self.partial_handle = io_unsafe.partial_read(self.db_name, self.key)
			if self.as_PathDict:
				self.dict = PathDict(self.partial_handle.key_value)
			else:
				self.dict = self.partial_handle.key_value
		except BaseException as e:
			self.write_lock.unlock()
			raise e
		return self, self.dict

	def __exit__(self, type, value, tb):
		self.write_lock.unlock()
		self.write_lock = None
		self.in_session = False

	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")
		io_unsafe.partial_write(self.partial_handle)