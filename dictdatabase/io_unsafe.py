from __future__ import annotations
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
	suffix: bytes


@dataclass(frozen=True)
class PartialFileHandle:
	db_name: str
	partial_dict: PartialDict
	indent_level: int
	indent_with: str
	indexer: indexing.Indexer


################################################################################
#### Reading
################################################################################


def read(db_name: str) -> dict:
	"""
		Read the file at db_path from the configured storage directory.
		Make sure the file exists. If it does notnot a FileNotFoundError is
		raised.
	"""
	# Always use orjson to read the file, because it is faster
	return orjson.loads(io_bytes.read(db_name))


def try_read_bytes_by_index(indexer: indexing.Indexer, db_name, key):
	if (index := indexer.get(key)) is None:
		return None
	start_index, end_index, _, _, value_hash = index
	partial_bytes = io_bytes.read(db_name, start_index, end_index)
	if value_hash != hashlib.sha256(partial_bytes).hexdigest():
		return None
	return orjson.loads(partial_bytes)


def partial_read_only(db_name: str, key: str) -> dict:
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
		raise KeyError(f"Key \"{key}\" not found in db \"{db_name}\"")

	# Key found, now determine the bounds of the value
	space_after_semicolon = 1 if file_bytes[key_end] == byte_codes.SPACE else 0
	value_start = key_end + space_after_semicolon
	value_end = utils.seek_index_through_value_bytes(file_bytes, value_start)

	indent_level, indent_with  = utils.detect_indentation_in_json_bytes(file_bytes, key_start)
	value_bytes = file_bytes[value_start:value_end]

	# Write key info to index file
	indexer.write(key, value_start, value_end, indent_level, indent_with,
		hashlib.sha256(value_bytes).hexdigest()
	)
	return orjson.loads(value_bytes)


def get_partial_file_handle(db_name: str, key: str) -> PartialFileHandle:
	"""
		Partially read a key from a db.
		The key MUST be unique in the entire db, otherwise the behavior is undefined.
		This is a lot faster than reading the entire db, because it does not parse
		the entire file, but only the part <value> part of the <key>: <value> pair.

		If the key is not found, a `KeyError` is raised.
	"""

	data = io_bytes.read(db_name)

	# Search for key in the index file
	indexer = indexing.Indexer(db_name)
	index = indexer.get(key)
	if index is not None:
		start_index, end_index, indent_level, indent_with, value_hash = index
		partial_bytes = data[start_index:end_index]
		if value_hash == hashlib.sha256(partial_bytes).hexdigest():
			partial_value = orjson.loads(partial_bytes)
			partial_dict = PartialDict(data[:start_index], key, partial_value, data[end_index:])
			return PartialFileHandle(db_name, partial_dict, indent_level, indent_with, indexer)

	# Not found in index file, search for key in the entire file
	key_start, key_end = utils.find_outermost_key_in_json_bytes(data, key)

	if key_end == -1:
		raise KeyError(f"Key \"{key}\" not found in db \"{db_name}\"")

	# Key found, now determine the bounds of the value
	space_after_semicolon = 1 if data[key_end] == byte_codes.SPACE else 0
	value_start = key_end + space_after_semicolon
	value_end = utils.seek_index_through_value_bytes(data, value_start)

	indent_level, indent_with  = utils.detect_indentation_in_json_bytes(data, key_start)
	partial_bytes = data[value_start:value_end]

	# Write key info to index file

	partial_value = orjson.loads(partial_bytes)
	partial_dict = PartialDict(data[:value_start], key, partial_value, data[value_end:])
	return PartialFileHandle(db_name, partial_dict, indent_level, indent_with, indexer)


################################################################################
#### Writing
################################################################################


def write(db_name: str, data: dict):
	"""
		Write the dict db dumped as a json string
		to the file of the db_path.
	"""
	if config.use_orjson:
		option = orjson.OPT_INDENT_2 if config.indent else 0
		option |= orjson.OPT_SORT_KEYS if config.sort_keys else 0
		db_dump = orjson.dumps(data, option=option)
	else:
		db_dump = json.dumps(data, indent=config.indent, sort_keys=config.sort_keys)
		db_dump = db_dump.encode()
	io_bytes.write(db_name, db_dump)


def partial_write(pf: PartialFileHandle):
	"""
		Write a partial file handle to the db.
	"""

	if config.use_orjson:
		option = orjson.OPT_INDENT_2 if config.indent else 0
		option |= orjson.OPT_SORT_KEYS if config.sort_keys else 0
		partial_dump = orjson.dumps(pf.partial_dict.value, option=option)
	else:
		partial_dump = json.dumps(pf.partial_dict.value, indent=config.indent, sort_keys=config.sort_keys)
		partial_dump = partial_dump.encode()

	# Add indentation
	if pf.indent_level > 0 and pf.indent_with:
		replace_this = "\n".encode()
		replace_with = ("\n" + (pf.indent_level * pf.indent_with)).encode()
		partial_dump = partial_dump.replace(replace_this, replace_with)

	pf.indexer.write(pf.partial_dict.key, len(pf.partial_dict.prefix),
		len(pf.partial_dict.prefix) + len(partial_dump), pf.indent_level,
		pf.indent_with, hashlib.sha256(partial_dump).hexdigest()
	)
	io_bytes.write(pf.db_name, pf.partial_dict.prefix + partial_dump + pf.partial_dict.suffix)
