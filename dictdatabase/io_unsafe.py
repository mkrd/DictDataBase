from __future__ import annotations
from dataclasses import dataclass
import orjson
import json
import zlib
import os
from . import config, utils


@dataclass(frozen=True)
class PartialFileHandle:
	db_name: str
	key: str
	key_value: dict
	value_start_index: int
	value_end_index: int
	original_data_str: str
	indent_level: int


################################################################################
#### Reading
################################################################################


def read_string(db_name: str) -> str:
	"""
		Read the content of a db as a string.
		Reading is always possible, not matter how the config is set.
		So a compressed ddb file can also be read if compression is disabled,
		and vice versa.
	"""
	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)

	if json_exists and ddb_exists:
		raise FileExistsError(f"DB Inconsistency: \"{db_name}\" exists as .json and .ddb")

	if not json_exists and not ddb_exists:
		raise FileNotFoundError(f"DB \"{db_name}\" does not exist.")

	# Read from json file
	if json_exists:
		with open(json_path, "r") as f:
			return f.read()
	# Read from compressed ddb file
	if ddb_exists:
		with open(ddb_path, "rb") as f:
			data_bytes = f.read()
			return zlib.decompress(data_bytes).decode()


def read(db_name: str) -> dict:
	"""
		Read the file at db_path from the configured storage directory.
		Make sure the file exists. If it does notnot a FileNotFoundError is
		raised.
	"""
	data = read_string(db_name)
	return orjson.loads(data) if config.use_orjson else json.loads(data)


def partial_read(db_name: str, key: str) -> PartialFileHandle:
	"""
		Partially read a key from a db.
		The key MUST be unique in the entire db, otherwise the behavior is undefined.
		This is a lot faster than reading the entire db, because it does not parse
		the entire file, but only the part <value> part of the <key>: <value> pair.

		If the key is not found, a `KeyError` is raised.
	"""

	data = read_string(db_name)
	key_str = f"\"{key}\":"
	key_str_index = utils.find_outermost_key_str_index(data, key_str)

	if key_str_index == -1:
		raise KeyError(f"Key \"{key}\" not found in db \"{db_name}\"")

	# Count the amount of whitespace before the key
	# to determine the indentation level
	indentation_level = 0
	for i in range(key_str_index-1, -1, -1):
		if data[i] not in [" ", "\t"]:
			break
		indentation_level += 1

	if isinstance(config.indent, int) and config.indent > 0:
		indentation_level //= config.indent

	value_start_index = key_str_index + len(key_str)
	value_end_index = utils.seek_index_through_value(data, value_start_index)

	return PartialFileHandle(
		db_name=db_name,
		key=key,
		key_value=json.loads(data[value_start_index:value_end_index]),
		value_start_index=value_start_index,
		value_end_index=value_end_index,
		original_data_str=data,
		indent_level=indentation_level,
	)


################################################################################
#### Writing
################################################################################


def write_dump(db_name: str, dump: str | bytes):
	"""
		Write the dump to the file of the db_path.
		If the db was compressed but now config.use_compression is False,
		remove the compressed file, and vice versa.
	"""
	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)
	# Write bytes or string to file
	if config.use_compression:
		write_path = ddb_path
		if json_exists:
			os.remove(json_path)
	else:
		write_path = json_path
		if ddb_exists:
			os.remove(ddb_path)

	if config.use_compression:
		dump = zlib.compress(dump if isinstance(dump, bytes) else dump.encode(), 1)

	# Write bytes or string to file
	open_mode = "wb" if isinstance(dump, bytes) else "w"
	with open(write_path, open_mode) as f:
		f.write(dump)


def write(db_name: str, db: dict):
	"""
		Write the dict db dumped as a json string
		to the file of the db_path.
	"""
	if config.use_orjson:
		orjson_indent = orjson.OPT_INDENT_2 if config.indent else 0
		orjson_sort_keys = orjson.OPT_SORT_KEYS if config.sort_keys else 0
		db_dump = orjson.dumps(db, option=orjson_indent | orjson_sort_keys)
	else:
		db_dump = json.dumps(db, indent=config.indent, sort_keys=config.sort_keys)

	write_dump(db_name, db_dump)


def partial_write(pf: PartialFileHandle):
	"""
		Write a partial file handle to the db.
	"""
	if config.use_orjson:
		orjson_indent = orjson.OPT_INDENT_2 if config.indent else 0
		orjson_sort_keys = orjson.OPT_SORT_KEYS if config.sort_keys else 0
		partial_dump = orjson.dumps(pf.key_value, option=orjson_indent | orjson_sort_keys)
		partial_dump = partial_dump.decode()
	else:
		partial_dump = json.dumps(pf.key_value, indent=config.indent, sort_keys=config.sort_keys)

	if config.indent is not None:
		indent_with = " " * config.indent if isinstance(config.indent, int) else config.indent
		partial_dump = partial_dump.replace("\n", "\n" + (pf.indent_level * indent_with))

	dump_start = pf.original_data_str[:pf.value_start_index]
	dump_end = pf.original_data_str[pf.value_end_index:]
	write_dump(pf.db_name, f"{dump_start} {partial_dump}{dump_end}")
