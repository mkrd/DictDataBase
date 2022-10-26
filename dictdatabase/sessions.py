from __future__ import annotations
from typing import Tuple, TypeVar, Generic
from . import utils, io_unsafe, locking


T = TypeVar("T")
JSONSerializable = TypeVar("JSONSerializable", str, int, float, bool, None, list, dict)


class DDBSession(Generic[T]):
	"""
		Enter:
		>>> with DDBSession(db_name) as session, data:

		Where `data` is the dict that was read from the filesystem. Modify
		`data` and call session.write() to save changes. If you don't call it,
		the changes will be lost after exiting the with statement.
	"""

	in_session: bool = False
	as_type: T

	def __init__(self, db_name: str, key: str = None, as_type: T = None):
		self.db_name = db_name
		self.as_type = as_type

		if key is not None:
			if "*" in key:
				raise ValueError("A key cannot be specified with a wildcard.")
			self.key = key
			self.session_type = "sub"
		elif "*" in db_name:
			self.db_name = utils.find(db_name)
			self.session_type = "multi"
		else:
			self.session_type = "single"

	def __enter__(self) -> Tuple["DDBSession", JSONSerializable | T]:
		"""
			Any number of read tasks can be carried out in parallel.
			Each read task creates a read lock while reading, to signal that it is reading.

			As soon as a session starts, it writes a wants-to-write lock,
			No new read tasks will be allowed. When all read tasks are done, the session aquire the write lock.
			Now, it can savely read and write while all other tasks wait.
		"""
		if self.session_type in ("single", "sub"):
			self.write_lock = locking.WriteLock(self.db_name)
		else:
			self.write_locks = [locking.WriteLock(x) for x in self.db_name]
		self.in_session = True

		try:
			if self.session_type == "single":
				self.data_handle = io_unsafe.read(self.db_name)
			elif self.session_type == "sub":
				self.partial_handle = io_unsafe.partial_read(self.db_name, self.key)
				self.data_handle = self.partial_handle.key_value
			elif self.session_type == "multi":
				self.data_handle = {n: io_unsafe.read(n) for n in self.db_name}


			if self.as_type is not None:
				return self, self.as_type(self.data_handle)
			return self, self.data_handle


		except BaseException as e:
			if self.session_type in ("single", "sub"):
				self.write_lock.unlock()
			else:
				for lock in self.write_locks:
					lock.unlock()
			raise e


	def __exit__(self, type, value, tb):
		if self.session_type in ("single", "sub"):
			self.write_lock.unlock()
			self.write_lock = None
		else:
			for lock in self.write_locks:
				lock.unlock()
			self.write_locks = None
		self.in_session = False


	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")

		if self.session_type == "single":
			io_unsafe.write(self.db_name, self.data_handle)
		elif self.session_type == "sub":
			io_unsafe.partial_write(self.partial_handle)
		else: # Multi
			for name in self.db_name:
				io_unsafe.write(name, self.data_handle[name])
