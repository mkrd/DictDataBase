from __future__ import annotations
from typing import Tuple
import os
import glob
from . import config


def db_paths(db_name: str) -> Tuple[str, bool, str, bool]:
	"""
	Returns a tuple of four elements, the first and third being the paths to the JSON
	and and DDB files, and the second and third being booleans indicating whether those
	files exist:

	>>> (json_path, json_exists, ddb_path, ddb_exists)

	:param db_name: The name of the database
	"""
	base = f"{config.storage_directory}/{db_name}"
	j, d = f"{base}.json", f"{base}.ddb"
	return j, os.path.exists(j), d, os.path.exists(d)


def to_path_str(s) -> str:
	"""
		Join a tuple or list using "/" as a separator.
		:param s: The string to convert to a path string.
	"""
	return "/".join(s) if isinstance(s, (tuple, list)) else s


def find(*pattern) -> list[str]:
	"""
	Returns a list of all the database names that match the given glob pattern.

	:param pattern: The glob pattern to search for
	"""
	pattern = to_path_str(pattern)
	dbs_ddb = glob.glob(f"{config.storage_directory}/{pattern}.ddb")
	dbs_json = glob.glob(f"{config.storage_directory}/{pattern}.json")
	dbs_all = dbs_ddb + dbs_json
	for trim in [f"{config.storage_directory}/", ".ddb", ".json"]:
		dbs_all = [d.replace(trim, "") for d in dbs_all]
	return dbs_all


def expand_find_path_pattern(path):
	"""
		:param str pattern: The pattern to expand.
		Fot a tuple of path items, expand it to a list of all real paths.
		An item can be some string, a wildcard "*" or a list to select specific paths.
	"""
	res = [[]]
	for item in path.split("/"):
		if isinstance(item, str):
			res = [r + [item] for r in res]
		if isinstance(item, list):
			res = [r + [list_item] for list_item in item for r in res]
	return [f for r in res for f in find(*r)]


def seek_index_through_value(data: str, index: int) -> int:
	"""
	Finds the index of the next comma or closing bracket/brace, but only if
	it is at the same indentation level as at the start index.

	:param data: The string to be parsed
	:param index: the index of the first character of the value
	"""
	in_str, list_depth, dict_depth = False, 0, 0

	d_prev, d_curr = None, data[index - 1]
	for i in range(index, len(data)):
		d_prev, d_curr = d_curr, data[i]
		prev_backslash = d_prev == "\\"
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


def count_nesting(data: str, start: int, end: int) -> int:
	"""
	Returns the number of nesting levels between the start and end indices.

	:param data: The string to be parsed
	"""
	in_str, nesting = False, 0

	d_prev, d_curr = None, data[start - 1]
	for i in range(start, end):
		d_prev, d_curr = d_curr, data[i]
		prev_backslash = d_prev == "\\"
		if d_curr == '"' and not prev_backslash:
			in_str = not in_str
			continue
		if in_str:
			continue
		elif d_curr == "{":
			nesting += 1
		elif d_curr == "}":
			nesting -= 1
	return nesting


def find_outermost_key_str_index(data: str, key_str: str):
	"""
		Returns the index of the key_str that is at the outermost nesting level.
	"""
	if (curr_i := data.find(key_str, 0)) == -1:
		return -1

	key_nest = [(curr_i, 0)]  # (key, nesting)

	while (next_i := data.find(key_str, curr_i + len(key_str))) != -1:
		nesting = count_nesting(data, curr_i + len(key_str), next_i)
		key_nest.append((next_i, nesting))
		curr_i = next_i

	# Early exit if there is only one key
	if len(key_nest) == 1:
		return key_nest[0][0]

	# Relative to total nesting
	for i in range(1, len(key_nest)):
		key_nest[i] = (key_nest[i][0], key_nest[i - 1][1] + key_nest[i][1])
	return min(key_nest, key=lambda x: x[1])[0]
