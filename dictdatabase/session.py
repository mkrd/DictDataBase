from __future__ import annotations
from typing import Tuple, TypeVar, Generic
from . import utils, io_unsafe, locking


T = TypeVar("T")
JSONSerializable = TypeVar("JSONSerializable", str, int, float, bool, None, list, dict)


class SessionType:
	SINGLE = 0
	MULTI = 1
	SUB = 2


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
			self.session_type = SessionType.SUB
		elif "*" in db_name:
			self.db_name = utils.find(db_name)
			self.session_type = SessionType.MULTI
		else:
			self.session_type = SessionType.SINGLE

	def __enter__(self) -> Tuple["DDBSession", JSONSerializable | T]:
		"""
			Any number of read tasks can be carried out in parallel.
			Each read task creates a read lock while reading, to signal that it is reading.

			As soon as a session starts, it writes a wants-to-write lock,
			No new read tasks will be allowed. When all read tasks are done, the session aquire the write lock.
			Now, it can savely read and write while all other tasks wait.
		"""
		if self.session_type in (SessionType.SINGLE, SessionType.SUB):
			self.write_lock = locking.WriteLock(self.db_name)
		else:
			self.write_lock = [locking.WriteLock(x) for x in self.db_name]
		self.in_session = True

		try:
			if self.session_type == SessionType.SINGLE:
				dh = io_unsafe.read(self.db_name)
				self.data_handle = dh
			elif self.session_type == SessionType.SUB:
				self.partial_handle = io_unsafe.partial_read(self.db_name, self.key)
				dh = self.partial_handle.key_value
				self.data_handle = dh
			elif self.session_type == SessionType.MULTI:
				self.data_handle = {n: io_unsafe.read(n) for n in self.db_name}
			return self, self.as_type(dh) if self.as_type is not None else dh
		except BaseException as e:
			if self.session_type == SessionType.MULTI:
				for lock in self.write_lock:
					lock.unlock()
			else:
				self.write_lock.unlock()
			raise e


	def __exit__(self, type, value, tb):
		if self.session_type == SessionType.MULTI:
			for lock in self.write_lock:
				lock.unlock()
		else:
			self.write_lock.unlock()
		self.write_lock, self.in_session = None, False


	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")
		if self.session_type == SessionType.SINGLE:
			io_unsafe.write(self.db_name, self.data_handle)
		elif self.session_type == SessionType.SUB:
			io_unsafe.partial_write(self.partial_handle)
		elif self.session_type == SessionType.MULTI:
			for name in self.db_name:
				io_unsafe.write(name, self.data_handle[name])
