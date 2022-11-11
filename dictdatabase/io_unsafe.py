from __future__ import annotations
from typing import Tuple
from dataclasses import dataclass
import orjson
import json
import hashlib
from . import config, utils, byte_codes, indexing, io_bytes


@dataclass(frozen=True)
class PartialDict:
	prefix: bytes
	key: str
	value: dict
	value_start: int
	value_end: int
	suffix: bytes


@dataclass(frozen=True)
class PartialFileHandle:
	db_name: str
	partial_dict: PartialDict
	indent_level: int
	indent_with: str
	indexer: indexing.Indexer


########################################################################################
#### Full Reading
########################################################################################


def read(db_name: str) -> dict:
	"""
		Read the file at db_path from the configured storage directory.
		Make sure the file exists. If it does notnot a FileNotFoundError is
		raised.
	"""
	# Always use orjson to read the file, because it is faster
	return orjson.loads(io_bytes.read(db_name))


########################################################################################
#### Partial Reading
########################################################################################


def try_read_bytes_by_index(indexer: indexing.Indexer, db_name, key):
	if (index := indexer.get(key)) is None:
		return None
	start_index, end_index, _, _, value_hash = index
	partial_bytes = io_bytes.read(db_name, start_index, end_index)
	if value_hash != hashlib.sha256(partial_bytes).hexdigest():
		return None
	return orjson.loads(partial_bytes)


def partial_read_only(db_name: str, key: str) -> dict | None:
	"""
		Partially read a key from a db.
		The key MUST be unique in the entire db, otherwise the behavior is undefined.
		This is a lot faster than reading the entire db, because it does not parse
		the entire file, but only the part <value> part of the <key>: <value> pair.

		If the key is not found, a `KeyError` is raised.
	"""

	# Search for key in the index file
	indexer = indexing.Indexer(db_name)
	if (value_data := try_read_bytes_by_index(indexer, db_name, key)) is not None:
		return value_data

	# Not found in index file, search for key in the entire file
	file_bytes = io_bytes.read(db_name)
	key_start, key_end = utils.find_outermost_key_in_json_bytes(file_bytes, key)

	if key_end == -1:
		return None

	# Key found, now determine the bounds of the value
	value_start = key_end + (1 if file_bytes[key_end] == byte_codes.SPACE else 0)
	value_end = utils.seek_index_through_value_bytes(file_bytes, value_start)

	indent_level, indent_with  = utils.detect_indentation_in_json_bytes(file_bytes, key_start)
	value_bytes = file_bytes[value_start:value_end]
	value_hash = hashlib.sha256(value_bytes).hexdigest()

	# Write key info to index file

	indexer.write(key, value_start, value_end, indent_level, indent_with, value_hash, value_end)
	return orjson.loads(value_bytes)


################################################################################
#### Writing
################################################################################


def serialize_data_to_json_bytes(data: dict) -> bytes:
	"""
		Serialize the data as json bytes. Depending on the config,
		this can be done with orjson or the standard json module.
		Additionally config.indent is respected.
	"""
	if config.use_orjson:
		option = (orjson.OPT_INDENT_2 if config.indent else 0) | orjson.OPT_SORT_KEYS
		return orjson.dumps(data, option=option)
	else:
		db_dump = json.dumps(data, indent=config.indent, sort_keys=True)
		return db_dump.encode()


def write(db_name: str, data: dict):
	"""
		Write the dict db dumped as a json string
		to the file of the db_path.
	"""
	data_bytes = serialize_data_to_json_bytes(data)
	io_bytes.write(db_name, data_bytes)


################################################################################
#### Partial Writing
################################################################################


def try_get_parial_file_handle_by_index(indexer: indexing.Indexer, db_name, key) -> Tuple[PartialFileHandle | None, bytes | None]:
	"""
		Try to get a partial file handle by using the key entry in the index file.

		If the data could be read from the index file, a tuple of the partial file
		handle and None is returned.
		If the data could not be read from the index file, a tuple of None and the file
		bytes is returned, so that the file bytes can be searched for the key.
	"""
	if (index := indexer.get(key)) is None:
		return None, io_bytes.read(db_name)
	value_start, value_end, indent_level, indent_with, value_hash = index

	# If compression is enabled, all data has to be read from the file
	if config.use_compression:
		data_bytes = io_bytes.read(db_name)
		value_bytes = data_bytes[value_start:value_end]
		if value_hash != hashlib.sha256(value_bytes).hexdigest():
			return None, data_bytes
		value_data = orjson.loads(value_bytes)
		partial_dict = PartialDict(data_bytes[:value_start], key, value_data, value_start, value_end, data_bytes[value_end:])

	# If compression is disabled, only the value and suffix have to be read
	else:
		value_and_suffix_bytes = io_bytes.read(db_name, value_start)
		value_length = value_end - value_start
		value_bytes = value_and_suffix_bytes[:value_length]
		if value_hash != hashlib.sha256(value_bytes).hexdigest():
			# If the hashes don't match, read the prefix to concat the full file bytes
			prefix_bytes = io_bytes.read(db_name, 0, value_start)
			return None, prefix_bytes + value_and_suffix_bytes
		value_data = orjson.loads(value_bytes)
		partial_dict = PartialDict(None, key, value_data, value_start, value_end, value_and_suffix_bytes[value_length:])

	return PartialFileHandle(db_name, partial_dict, indent_level, indent_with, indexer), None




def get_partial_file_handle(db_name: str, key: str) -> PartialFileHandle:
	"""
		Partially read a key from a db.
		The key MUST be unique in the entire db, otherwise the behavior is undefined.
		This is a lot faster than reading the entire db, because it does not parse
		the entire file, but only the part <value> part of the <key>: <value> pair.

		If the key is not found, a `KeyError` is raised.
	"""

	# Search for key in the index file
	indexer = indexing.Indexer(db_name)
	partial_handle, data_bytes = try_get_parial_file_handle_by_index(indexer, db_name, key)
	if partial_handle is not None:
		return partial_handle

	# Not found in index file, search for key in the entire file
	key_start, key_end = utils.find_outermost_key_in_json_bytes(data_bytes, key)

	if key_end == -1:
		raise KeyError(f"Key \"{key}\" not found in db \"{db_name}\"")

	# Key found, now determine the bounds of the value
	value_start = key_end + (1 if data_bytes[key_end] == byte_codes.SPACE else 0)
	value_end = utils.seek_index_through_value_bytes(data_bytes, value_start)

	indent_level, indent_with  = utils.detect_indentation_in_json_bytes(data_bytes, key_start)

	partial_value = orjson.loads(data_bytes[value_start:value_end])
	prefix_bytes = data_bytes[:value_start] if config.use_compression else None
	partial_dict = PartialDict(prefix_bytes, key, partial_value, value_start, value_end, data_bytes[value_end:])
	return PartialFileHandle(db_name, partial_dict, indent_level, indent_with, indexer)


def partial_write(pf: PartialFileHandle):
	"""
		Write a partial file handle to the db.
	"""

	partial_bytes = serialize_data_to_json_bytes(pf.partial_dict.value)

	# Add indentation
	if pf.indent_level > 0 and pf.indent_with:
		replace_this = "\n".encode()
		replace_with = ("\n" + (pf.indent_level * pf.indent_with)).encode()
		partial_bytes = partial_bytes.replace(replace_this, replace_with)

	# Write key info to index file
	pf.indexer.write(
		key=pf.partial_dict.key,
		start_index=pf.partial_dict.value_start,
		end_index=pf.partial_dict.value_start + len(partial_bytes),
		indent_level=pf.indent_level,
		indent_with=pf.indent_with,
		value_hash=hashlib.sha256(partial_bytes).hexdigest(),
		old_value_end=pf.partial_dict.value_end,
	)

	if pf.partial_dict.prefix is None:
		# Prefix could not be determined due to compression, so write the entire file
		io_bytes.write(pf.db_name, partial_bytes + pf.partial_dict.suffix, start=pf.partial_dict.value_start)
	else:
		# Prefix was determined, so only write the changed part and the suffix
		io_bytes.write(pf.db_name, pf.partial_dict.prefix + partial_bytes + pf.partial_dict.suffix)
