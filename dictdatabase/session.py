from __future__ import annotations
from typing import Tuple, TypeVar, Generic, Any, Callable
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

	def __init__(self, db_name: str, op_type, key: str = None, where: Callable[[Any, Any], bool] = None, as_type: T = None):
		self.db_name = db_name
		self.as_type = as_type
		self.where = where
		self.key = key
		self.op_type = op_type

		if op_type.dir:
			self.db_name = utils.find(db_name)

	def __enter__(self) -> Tuple["DDBSession", JSONSerializable | T]:
		"""
			Any number of read tasks can be carried out in parallel.
			Each read task creates a read lock while reading, to signal that it is reading.

			As soon as a session starts, it writes a wants-to-write lock,
			No new read tasks will be allowed. When all read tasks are done, the session aquire the write lock.
			Now, it can savely read and write while all other tasks wait.
		"""
		self.in_session = True
		self.data_handle = {}

		try:

			if self.op_type.file_normal:
				self.write_lock = locking.WriteLock(self.db_name)
				self.write_lock._lock()
				self.data_handle = io_unsafe.read(self.db_name)

			elif self.op_type.file_key:
				self.write_lock = locking.WriteLock(self.db_name)
				self.write_lock._lock()
				self.partial_handle = io_unsafe.get_partial_file_handle(self.db_name, self.key)
				self.data_handle = self.partial_handle.partial_dict.value

			elif self.op_type.file_where:
				self.write_lock = locking.WriteLock(self.db_name)
				self.write_lock._lock()
				self.original_data = io_unsafe.read(self.db_name)
				for k, v in self.original_data.items():
					if self.where(k, v):
						self.data_handle[k] = v

			elif self.op_type.dir_normal:
				self.write_lock = [locking.WriteLock(x) for x in self.db_name]
				for lock in self.write_lock:
					lock._lock()
				self.data_handle = {n.split("/")[-1]: io_unsafe.read(n) for n in self.db_name}

			elif self.op_type.dir_where:
				selected_db_names, write_lock = [], []
				for db_name in self.db_name:
					lock = locking.WriteLock(db_name)
					lock._lock()
					k, v = db_name.split("/")[-1], io_unsafe.read(db_name)
					if self.where(k, v):
						self.data_handle[k] = v
						write_lock.append(lock)
						selected_db_names.append(db_name)
					else:
						lock._unlock()
				self.write_lock = write_lock
				self.db_name = selected_db_names

			casted = self.data_handle
			if self.as_type is not None:
				casted = self.as_type(self.data_handle)
			return self, casted

		except BaseException as e:
			self.__exit__(type(e), e, e.__traceback__)
			raise e



	def __exit__(self, type, value, tb):
		if self.op_type.dir:
			# Use getattr in case the attr doesn't exist
			for lock in getattr(self, "write_lock", []):
				lock._unlock()
		elif getattr(self, "write_lock", None) is not None:
			self.write_lock._unlock()
		self.write_lock, self.in_session = None, False



	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")
		elif self.op_type.file_normal:
			io_unsafe.write(self.db_name, self.data_handle)
		elif self.op_type.file_key:
			io_unsafe.partial_write(self.partial_handle)
		elif self.op_type.file_where:
			self.original_data.update(self.data_handle)
			io_unsafe.write(self.db_name, self.original_data)
		elif self.op_type.dir_normal or self.op_type.dir_where:
			for name in self.db_name:
				io_unsafe.write(name, self.data_handle[name.split("/")[-1]])
