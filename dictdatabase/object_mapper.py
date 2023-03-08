from __future__ import annotations
from abc import ABC
from typing import get_type_hints, get_origin, get_args, TypeVar, Tuple, Type, Generic
from types import UnionType, NoneType
from . import io_safe
from .sessions import SessionFileFull, SessionFileKey, SessionFileWhere, SessionDirFull, SessionDirWhere



T = TypeVar("T")
T2 = TypeVar("T2")


def get_type_hints_excluding_internals(cls):
	"""
		Get type hints of the class, excluding double dunder variables.
	"""
	for var_name, var_type in get_type_hints(cls).items():
		if var_name.startswith("__") and var_name.endswith("__"):
			continue
		yield var_name, var_type



def fill_object_from_dict_using_type_hints(obj, cls, data: dict):
	"""
		Attributes of obj are set using the data dict.
		The type hints of the class cls are used to determine which attributes to set.
	"""
	for var_name, var_type in get_type_hints_excluding_internals(cls):
		var_type_args = get_args(var_type)
		var_type_origin = get_origin(var_type)
		# Check if variable is nullable (e.g. email: str | None)
		# When it is not nullable but not in the data, raise an error
		if var_name not in data:
			nullable = var_type_origin is UnionType and NoneType in var_type_args
			if not nullable:
				raise KeyError(f"Missing variable '{var_name}' in {cls.__name__}.")
		# When it is a list, fill the list with the items
		if var_type_origin is list and len(var_type_args) == 1:
			item_type = var_type_args[0]
			setattr(obj, var_name, [item_type.from_dict(x) for x in data[var_name]])
		else:
			setattr(obj, var_name, data.get(var_name, None))
	return obj



def fill_dict_from_object_using_type_hints(cls, obj):
	raise NotImplementedError





########################################################################################
# Scenario 1:
# Model a single file with FileDictModel, which is a dict at the top level.
# Each key-value item is modeled by a DictItemModel.



class FileDictModel(ABC, Generic[T]):
	"""
		A file base refers to a file that is stored in the database.
		At the top level the file must contain a dictionary with strings as keys.
	"""

	__file__ = None

	@classmethod
	def _get_item_model(cls):
		for base in cls.__orig_bases__:
			for type_args in get_args(base):
				if issubclass(type_args, FileDictItemModel):
					return type_args
		raise AttributeError(
			"FileDictModel must specify a FileDictItemModel "
			"(e.g. Users(FileDictModel[User]))"
		)


	@classmethod
	def get_at_key(cls, key) -> T:
		"""
			Gets an item by key.
			The data is partially read from the __file__.
		"""
		data = io_safe.partial_read(cls.__file__, key)
		res: T = cls._get_item_model().from_key_value(key, data)
		return res

	@classmethod
	def session_at_key(cls, key):
		return cls._get_item_model().session(key)

	@classmethod
	def get_all(cls) -> dict[str, T]:
		data = io_safe.read(cls.__file__)
		return {k: cls._get_item_model().from_key_value(k, v) for k, v in data.items()}

	@classmethod
	def session(cls):
		"""
		Enter a session with the file as (session, data) where data is a dict of
		<key>: <ORM model of value> pairs.
		"""
		def make_session_obj_from_dict(data):
			sess_obj = {}
			for k, v in data.items():
				sess_obj[k] = cls._get_item_model().from_key_value(k, v)
			return sess_obj
		return SessionFileFull(cls.__file__, make_session_obj_from_dict)


	@classmethod
	def get_where(cls, where: callable[str, T]) -> dict[str, T]:
		"""
		Return a dictionary of all the items for which the where function returns True.
		Where takes the key and the value's model object as arguments.
		"""
		return {k: v for k, v in cls.get_all().items() if where(k, v)}




class FileDictItemModel(ABC):
	__key__: str

	@classmethod
	def from_key_value(cls: Type[T2], key, value) -> T2:
		obj = fill_object_from_dict_using_type_hints(cls(), cls, value)
		obj.__key__ = key
		return obj

	@classmethod
	def session(cls, key):
		def partial_func(x):
			return cls.from_key_value(key, x)
		return SessionFileKey(cls.__file__, key, partial_func)



class DictModel(ABC):

	@classmethod
	def from_dict(cls, data) -> DictModel:
		obj = cls()
		return fill_object_from_dict_using_type_hints(obj, cls, data)

	def to_dict(self) -> dict:
		res = {}
		for var_name in get_type_hints(self).keys():
			if (value := getattr(self, var_name)) is not None:
				res[var_name] = value
		return res



########################################################################################
# Scenario 2:
# Add in a later version of DDB
# A folder containing multiple files, each containing json.

# class FolderBase(ABC):
# 	__folder__ = None
# 	__file_model__: FileInFolderModel = None


# class FileInFolderModel(ABC):

# 	@classmethod
# 	def get_by_name(cls, file_name: str) -> FileInFolderModel:
# 		data = io_safe.read(f"{cls.__folder__}/{file_name}")
# 		return cls(**data)
