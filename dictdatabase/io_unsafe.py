import os
import json
import zlib
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

	# Use custom decoder if provided
	if config.custom_json_decoder is not None:
		return config.custom_json_decoder(data_str)
	# Otherwise, use default json decoder
	return json.loads(data_str)


def write(db_name: str, db: dict):
	"""
		Write the dict db dumped as a json string
		to the file of the db_path.
	"""
	json_path, json_exists, ddb_path, ddb_exists = utils.db_paths(db_name)

	# Dump db dict as string
	db_dump: str | bytes = None

	# Use custom encoder if provided
	if config.custom_json_encoder is not None and not config.use_compression:
		db_dump = config.custom_json_encoder(db)
	# Only generate pretty json if compression is disabled
	elif config.pretty_json_files and not config.use_compression:
		db_dump = json.dumps(db, indent="\t", sort_keys=True)
	# Generate compact json
	else:
		db_dump = json.dumps(db)

	if config.use_compression:
		write_path = ddb_path
		if json_exists:
			os.remove(json_path)
		db_dump = zlib.compress(db_dump if isinstance(db_dump, bytes) else db_dump.encode(), 1)
	else:
		write_path = json_path
		if ddb_exists:
			os.remove(ddb_path)

	# Write bytes or string to file
	open_mode = "wb" if isinstance(db_dump, bytes) else "w"
	with open(write_path, open_mode) as f:
		f.write(db_dump)





def seek_i_through_value(data_str, index):
	in_str = False
	in_lst = 0
	in_dct = 0
	for i in range(index, len(data_str)):
		if data_str[i] == '"' and data_str[i-1] != "\\":
			in_str = not in_str
			continue
		if in_str or data_str[i] == " ":
			continue
		if data_str[i] == "[":
			in_lst += 1
		elif data_str[i] == "]":
			in_lst -= 1
		elif data_str[i] == "{":
			in_dct += 1
		elif data_str[i] == "}":
			in_dct -= 1
		if in_lst == 0 and in_dct == 0:
			i += 1
			if data_str[i] == ",":
				i += 1
			break
	return i






def get_top_level_keys(data_str: str):
	"""
		Get the top level keys and their index in a json string.
	"""
	key_index = 0
	while (key_index := data_str.find('"', key_index)) != -1:
		# Find the end of the key
		key_end_index = data_str.find('"', key_index + 1)
		key = data_str[key_index + 1:key_end_index]
		# Find the next key
		value_end_index = seek_i_through_value(data_str, key_end_index + 2)
		yield key, key_end_index, value_end_index
		key_index = value_end_index





def partial_read(db_name: str, key: str) -> tuple[dict, (str, int, int)]:
	"""
		Read the file at db_path from the configured storage directory.
		Make sure the file exists!
	"""
	return read(db_name=db_name)[key], 0, 0
	data_str = read_string(db_name)

	for tlk, value_start_index, value_end_index in get_top_level_keys(data_str):
		if tlk != key:
			continue
		return json.loads(data_str[value_start_index+2:value_end_index-1]), value_start_index, value_end_index

	raise KeyError(f"Key \"{key}\" not found in db \"{db_name}\"")
