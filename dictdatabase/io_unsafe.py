import os
import json
import orjson
import zlib
from dataclasses import dataclass
from . import config, utils


def read_string(db_name: str) -> str:
	"""
		Read the content of a db as a string.
	"""
	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)

	if json_exists and ddb_exists:
		raise FileExistsError(f"DB Inconsistency: \"{db_name}\" exists as .json and .ddb")

	if not json_exists and not ddb_exists:
		raise FileNotFoundError(f"DB \"{db_name}\" does not exist.")


	data_str: str = None
	# Uncompressed json
	if json_exists:
		with open(json_path, "r") as f:
			data_str = f.read()
	# Compressed ddb
	elif ddb_exists:
		with open(ddb_path, "rb") as f:
			data_bytes = f.read()
			data_str = zlib.decompress(data_bytes).decode()
	return data_str


def read(db_name: str) -> dict:
	"""
		Read the file at db_path from the configured storage directory.
		Make sure the file exists!
	"""
	data_str = read_string(db_name)
	return orjson.loads(data_str) if config.use_orjson else json.loads(data_str)


def write_dump(db_name: str, dump: str | bytes):
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
		orjson_indent = orjson.OPT_INDENT_2 if config.indent_with else 0
		orjson_sort_keys = orjson.OPT_SORT_KEYS if config.sort_keys else 0
		db_dump = orjson.dumps(db, option=orjson_indent | orjson_sort_keys)
	else:
		json_indent = "\t" if config.indent_with == "\t" else len(config.indent_with)
		db_dump = json.dumps(db, indent=json_indent, sort_keys=config.sort_keys)

	if config.use_compression:
		db_dump = zlib.compress(db_dump if isinstance(db_dump, bytes) else db_dump.encode(), 1)

	write_dump(db_name, db_dump)



def seek_i_through_value(data_str, index):
	in_str = False
	list_depth = 0
	dict_depth = 0

	for i in range(index, len(data_str)):
		d_curr = data_str[i]
		prev_backslash = data_str[i-1] == "\\"
		if d_curr == '"' and not prev_backslash:
			in_str = not in_str
			continue
		if in_str or d_curr == " " or prev_backslash:
			continue
		if d_curr == "[":
			list_depth += 1
		elif d_curr == "]":
			list_depth -= 1
		elif d_curr == "{":
			dict_depth += 1
		elif d_curr == "}":
			dict_depth -= 1
		if list_depth == 0 and dict_depth == 0:
			return i + 1



@dataclass(frozen=True)
class PartialFileHandle:
	db_name: str
	key: str
	key_value: dict
	value_start_index: int
	value_end_index: int
	original_data_str: str
	indent_level: int
	indent_char: str




def partial_read(db_name: str, key: str) -> PartialFileHandle:
	"""
		Partially read a key from a db.
		The key MUST be unique in the entire db.
	"""

	data_str = read_string(db_name)
	key_str = f"\"{key}\":"
	key_str_index = data_str.find(key_str)

	if key_str_index == -1:
		raise KeyError(f"Key \"{key}\" not found in db \"{db_name}\"")

	# Count the amount of whitespace before the key
	# to determine the indentation level
	indentation_level = 0
	indentation_char = None
	for i in range(key_str_index-1, -1, -1):
		if data_str[i] not in [" ", "\t"]:
			break
		indentation_level += 1
		indentation_char = data_str[i]

	if indentation_char == " ":
		indentation_level //= len(config.indent_with)

	value_start_index = key_str_index + len(key_str)
	value_end_index = seek_i_through_value(data_str, value_start_index)

	return PartialFileHandle(
		db_name=db_name,
		key=key,
		key_value=json.loads(data_str[value_start_index:value_end_index]),
		value_start_index=value_start_index,
		value_end_index=value_end_index,
		original_data_str=data_str,
		indent_level=indentation_level,
		indent_char=indentation_char
	)





def partial_write(pf: PartialFileHandle):
	"""
		Write a partial file handle to the db.
	"""
	if config.use_orjson:
		config.indent_with = " " * 2
		orjson_indent = orjson.OPT_INDENT_2 if config.indent_with else 0
		orjson_sort_keys = orjson.OPT_SORT_KEYS if config.sort_keys else 0
		partial_dump = orjson.dumps(pf.key_value, option=orjson_indent | orjson_sort_keys)
		partial_dump = partial_dump.decode()
	else:
		json_indent = "\t" if config.indent_with == "\t" else len(config.indent_with)
		partial_dump = json.dumps(pf.key_value, indent=json_indent, sort_keys=config.sort_keys)


	indent = (pf.indent_level // len(config.indent_with)) * config.indent_with
	partial_dump = partial_dump.replace("\n", "\n" + indent)

	dump_start = pf.original_data_str[:pf.value_start_index]
	dump_end = pf.original_data_str[pf.value_end_index:]
	write_dump(pf.db_name, f"{dump_start} {partial_dump}{dump_end}")
