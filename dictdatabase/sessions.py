from __future__ import annotations
from typing import Tuple, Any, TypeVar
from . import utils, io_unsafe, locking

T = TypeVar("T")


class AbstractDDBSession:
	in_session: bool = False
	as_type: T

	def __init__(self, db_name: str, as_type: T = None):
		self.db_name = db_name
		self.as_type = as_type

	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")



class DDBSession(AbstractDDBSession):
	"""
		Enter:
		>>> with DDBSession(db_name) as session, data:

		Where `data` is the dict that was read from the filesystem. Modify
		`data` and call session.write() to save changes. If you don't call it,
		the changes will be lost after exiting the with statement.
	"""

	def __enter__(self) -> Tuple["DDBSession", dict | T]:
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
			self.data_handle = io_unsafe.read(self.db_name)
			if self.as_type is not None:
				return self, self.as_type(self.data_handle)
			return self, self.data_handle
		except BaseException as e:
			self.write_lock.unlock()
			raise e

	def __exit__(self, type, value, tb):
		self.write_lock.unlock()
		self.write_lock, self.in_session = None, False

	def write(self):
		super().write()
		io_unsafe.write(self.db_name, self.data_handle)


class DDBMultiSession(AbstractDDBSession):
	def __init__(self, pattern: str, as_type: T = None):
		super().__init__(utils.find(pattern), as_type)

	def __enter__(self) -> Tuple["DDBMultiSession", dict | T]:
		self.write_locks = [locking.WriteLock(x) for x in self.db_name]
		self.in_session = True
		try:
			self.data_handle = {n: io_unsafe.read(n) for n in self.db_name}
			if self.as_type is not None:
				return self, self.as_type(self.data_handle)
			return self, self.data_handle
		except BaseException as e:
			for write_lock in self.write_locks:
				write_lock.unlock()
			raise e

	def __exit__(self, type, value, tb):
		for write_lock in self.write_locks:
			write_lock.unlock()
		self.write_locks, self.in_session = None, False

	def write(self):
		super().write()
		for name in self.db_name:
			io_unsafe.write(name, self.data_handle[name])


class DDBSubSession(AbstractDDBSession):
	def __init__(self, db_name: str, key: str, as_type: T = None):
		super().__init__(db_name, as_type)
		self.key = key

	def __enter__(self) -> Tuple["DDBSubSession", dict | T]:
		self.write_lock = locking.WriteLock(self.db_name)
		self.in_session = True
		try:
			self.partial_handle = io_unsafe.partial_read(self.db_name, self.key)
			if self.as_type is not None:
				return self, self.as_type(self.partial_handle.key_value)
			return self, self.partial_handle.key_value
		except BaseException as e:
			self.write_lock.unlock()
			raise e

	def __exit__(self, type, value, tb):
		self.write_lock.unlock()
		self.write_lock, self.in_session = None, False

	def write(self):
		super().write()
		io_unsafe.partial_write(self.partial_handle)
