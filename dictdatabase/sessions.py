from __future__ import annotations
from typing import Tuple, TypeVar, Generic, Any, Callable
from . import utils, io_unsafe, locking

from contextlib import contextmanager


T = TypeVar("T")
JSONSerializable = TypeVar("JSONSerializable", str, int, float, bool, None, list, dict)



def type_cast(obj, as_type):
	return obj if as_type is None else as_type(obj)



class SessionBase:
	in_session: bool
	db_name: str
	as_type: T

	def __init__(self, db_name: str, as_type):
		self.in_session = False
		self.db_name = db_name
		self.as_type = as_type

	def __enter__(self):
		self.in_session = True
		self.data_handle = {}

	def __exit__(self, type, value, tb):
		write_lock = getattr(self, "write_lock", None)
		if write_lock is not None:
			if isinstance(write_lock, list):
				for lock in write_lock:
					lock._unlock()
			else:
				write_lock._unlock()
		self.write_lock, self.in_session = None, False

	def write(self):
		if not self.in_session:
			raise PermissionError("Only call write() inside a with statement.")



@contextmanager
def safe_context(super, self, *, db_names_to_lock=None):
	"""
		If an exception happens in the context, the __exit__ method of the passed super
		class will be called.
	"""
	super.__enter__()
	try:
		if isinstance(db_names_to_lock, str):
			self.write_lock = locking.WriteLock(self.db_name)
			self.write_lock._lock()
		elif isinstance(db_names_to_lock, list):
			self.write_lock = [locking.WriteLock(x) for x in self.db_name]
			for lock in self.write_lock:
				lock._lock()
		yield
	except BaseException as e:
		super.__exit__(type(e), e, e.__traceback__)
		raise e



########################################################################################
#### File sessions
########################################################################################



class SessionFileFull(SessionBase, Generic[T]):
	"""
		Context manager for read-write access to a full file.

		Efficiency:
		Reads and writes the entire file.
	"""

	def __enter__(self) -> Tuple[SessionFileFull, JSONSerializable | T]:
		with safe_context(super(), self, db_names_to_lock=self.db_name):
			self.data_handle = io_unsafe.read(self.db_name)
			return self, type_cast(self.data_handle, self.as_type)

	def write(self):
		super().write()
		io_unsafe.write(self.db_name, self.data_handle)



class SessionFileKey(SessionBase, Generic[T]):
	"""
		Context manager for read-write access to a single key-value item in a file.

		Efficiency:
		Uses partial reading, which allows only reading the bytes of the key-value item.
		When writing, only the bytes of the key-value and the bytes of the file after
		the key-value are written.
	"""

	def __init__(self, db_name: str, key: str, as_type: T):
		super().__init__(db_name, as_type)
		self.key = key

	def __enter__(self) -> Tuple[SessionFileKey, JSONSerializable | T]:
		with safe_context(super(), self, db_names_to_lock=self.db_name):
			self.partial_handle = io_unsafe.get_partial_file_handle(self.db_name, self.key)
			self.data_handle = self.partial_handle.partial_dict.value
			return self, type_cast(self.data_handle, self.as_type)

	def write(self):
		super().write()
		io_unsafe.partial_write(self.partial_handle)



class SessionFileWhere(SessionBase, Generic[T]):
	"""
		Context manager for read-write access to selection of key-value items in a file.
		The where callable is called with the key and value of each item in the file.

		Efficiency:
		Reads and writes the entire file, so it is not more efficient than
		SessionFileFull.
	"""
	def __init__(self, db_name: str, where: Callable[[Any, Any], bool], as_type: T):
		super().__init__(db_name, as_type)
		self.where = where

	def __enter__(self) -> Tuple[SessionFileWhere, JSONSerializable | T]:
		with safe_context(super(), self, db_names_to_lock=self.db_name):
			self.original_data = io_unsafe.read(self.db_name)
			for k, v in self.original_data.items():
				if self.where(k, v):
					self.data_handle[k] = v
			return self, type_cast(self.data_handle, self.as_type)

	def write(self):
		super().write()
		self.original_data.update(self.data_handle)
		io_unsafe.write(self.db_name, self.original_data)



########################################################################################
#### File sessions
########################################################################################



class SessionDirFull(SessionBase, Generic[T]):
	"""
		Context manager for read-write access to all files in a directory.
		They are provided as a dict of {str(file_name): dict(file_content)}, where the
		file name does not contain the directory name nor the file extension.

		Efficiency:
		Fully reads and writes all files.
	"""
	def __init__(self, db_name: str, as_type: T):
		super().__init__(utils.find_all(db_name), as_type)

	def __enter__(self) -> Tuple[SessionDirFull, JSONSerializable | T]:
		with safe_context(super(), self, db_names_to_lock=self.db_name):
			self.data_handle = {n.split("/")[-1]: io_unsafe.read(n) for n in self.db_name}
			return self, type_cast(self.data_handle, self.as_type)

	def write(self):
		super().write()
		for name in self.db_name:
			io_unsafe.write(name, self.data_handle[name.split("/")[-1]])



class SessionDirWhere(SessionBase, Generic[T]):
	"""
		Context manager for read-write access to selection of files in a directory.
		The where callable is called with the file name and parsed content of each file.

		Efficiency:
		Fully reads all files, but only writes the selected files.
	"""
	def __init__(self, db_name: str, where: Callable[[Any, Any], bool], as_type: T):
		super().__init__(utils.find_all(db_name), as_type)
		self.where = where

	def __enter__(self) -> Tuple[SessionDirWhere, JSONSerializable | T]:
		with safe_context(super(), self):
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
			return self, type_cast(self.data_handle, self.as_type)

	def write(self):
		super().write()
		for name in self.db_name:
			io_unsafe.write(name, self.data_handle[name.split("/")[-1]])
