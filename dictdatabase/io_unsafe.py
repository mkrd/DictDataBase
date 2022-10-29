from __future__ import annotations
from dataclasses import dataclass
import orjson
import json
import zlib
import os
import hashlib
from pathlib import Path
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
	indent_with: str


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



def read_index_file(db_name: str):
	path = f"{config.storage_directory}/.ddb/{db_name.replace('/', '___')}.index"
	Path(path).parent.mkdir(parents=True, exist_ok=True)
	if not os.path.exists(path):
		return {}
	with open(path, "r") as f:
		return json.load(f)


def write_index_file(db_name: str, key, start_index, end_index, indent_level, indent_with, value_hash):
	path = f"{config.storage_directory}/.ddb/{db_name}.index"
	Path(path).parent.mkdir(parents=True, exist_ok=True)
	indices = read_index_file(db_name)
	indices[key] = [start_index, end_index, indent_level, indent_with, value_hash]
	with open(path, "w") as f:
		json.dump(indices, f)



def partial_read(db_name: str, key: str) -> PartialFileHandle:
	"""
		Partially read a key from a db.
		The key MUST be unique in the entire db, otherwise the behavior is undefined.
		This is a lot faster than reading the entire db, because it does not parse
		the entire file, but only the part <value> part of the <key>: <value> pair.

		If the key is not found, a `KeyError` is raised.
	"""

	data = read_string(db_name)
	index = read_index_file(db_name).get(key, None)
	if index is not None:
		partial_str = data[index[0]:index[1]]
		if index[4] == hashlib.sha256(partial_str.encode()).hexdigest():
			return PartialFileHandle(
				db_name=db_name,
				key=key,
				key_value=json.loads(partial_str),
				value_start_index=index[0],
				value_end_index=index[1],
				indent_level=index[2],
				indent_with=index[3],
				original_data_str=data,
			)

	key_str = f"\"{key}\":"
	key_str_index = utils.find_outermost_key_str_index(data, key_str)

	if key_str_index == -1:
		raise KeyError(f"Key \"{key}\" not found in db \"{db_name}\"")

	space_after_semicolon = 1 if data[key_str_index + len(key_str)] == " " else 0

	# Count the amount of whitespace before the key
	# to determine the indentation level
	indentation_str = ""
	for i in range(key_str_index-1, -1, -1):
		if data[i] not in [" ", "\t"]:
			break
		indentation_str += data[i]

	if "\t" in indentation_str:
		indent_with = "\t"
		indent_level = len(indentation_str)
	elif isinstance(config.indent, int) and config.indent > 0:
		indent_with = " " * (len(indentation_str) // config.indent)
		indent_level = len(indentation_str) // config.indent
	elif isinstance(config.indent, str):
		indent_with = "  "
		indent_level = len(indentation_str) // 2
	else:
		indent_with, indent_level = "", 0


	value_start_index = key_str_index + len(key_str) + space_after_semicolon
	value_end_index = utils.seek_index_through_value(data, value_start_index)

	write_index_file(db_name, key, value_start_index, value_end_index, indent_level, indent_with, hashlib.sha256(data[value_start_index:value_end_index].encode()).hexdigest())

	return PartialFileHandle(
		db_name=db_name,
		key=key,
		key_value=json.loads(data[value_start_index:value_end_index]),
		value_start_index=value_start_index,
		value_end_index=value_end_index,
		original_data_str=data,
		indent_level=indent_level,
		indent_with=indent_with,
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

	if pf.indent_level > 0 and pf.indent_with:
		partial_dump = partial_dump.replace("\n", "\n" + (pf.indent_level * pf.indent_with))

	dump_start = pf.original_data_str[:pf.value_start_index]
	dump_end = pf.original_data_str[pf.value_end_index:]
	write_index_file(pf.db_name, pf.key, len(dump_start), len(dump_start) + len(partial_dump), pf.indent_level, pf.indent_with, hashlib.sha256(partial_dump.encode()).hexdigest())
	write_dump(pf.db_name, f"{dump_start}{partial_dump}{dump_end}")
