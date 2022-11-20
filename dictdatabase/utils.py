from __future__ import annotations
from typing import Tuple
import os
import glob
from . import config, byte_codes


def file_info(db_name: str) -> Tuple[str, bool, str, bool]:
	"""
		Returns a tuple of four elements, the first and third being the paths to the
		JSON and DDB files, and the second and third being booleans indicating whether
		those files exist:

		>>> (json_path, json_exists, ddb_path, ddb_exists)

		Args:
		- `db_name`: The name of the database
	"""
	base = f"{config.storage_directory}/{db_name}"
	j, d = f"{base}.json", f"{base}.ddb"
	return j, os.path.exists(j), d, os.path.exists(d)


def find_all(file_name: str) -> list[str]:
	"""
	Returns a list of all the database names that match the given glob file_name.

	Args:
	- `file_name`: The glob file_name to search for
	"""

	files_all = glob.glob(f"{config.storage_directory}/{file_name}.ddb")
	files_all += glob.glob(f"{config.storage_directory}/{file_name}.json")

	for trim in [f"{config.storage_directory}/", ".ddb", ".json"]:
		files_all = [d.replace(trim, "") for d in files_all]
	return files_all


def seek_index_through_value_bytes(json_bytes: bytes, index: int) -> int:
	"""
	Finds the index of the next comma or closing bracket/brace after the value
	of a key-value pair in a bytes object containing valid JSON when decoded.

	Args:
	- `json_bytes`: A bytes object containing valid JSON when decoded
	- `index`: The start index in json_bytes

	Returns:
	- The end index of the value.
	"""

	# See https://www.json.org/json-en.html for the JSON syntax

	skip_next, in_str, list_depth, dict_depth = False, False, 0, 0


	i = index
	while i < len(json_bytes):
		current = json_bytes[i]

		# If backslash, skip the next character
		if current == byte_codes.BACKSLASH:
			i += 1
		# If quote, toggle in_str
		elif current == byte_codes.QUOTE:
			# Possible exit point where string ends and nesting is zero
			in_str = not in_str
			if not in_str and list_depth == 0 and dict_depth == 0:
				return i + 1
		# If in string, skip
		elif in_str:
			pass

		# Invariant: Not in_str, not escaped

		# Handle opening brackets
		elif current == byte_codes.OPEN_SQUARE:
			list_depth += 1
		elif current == byte_codes.OPEN_CURLY:
			dict_depth += 1

		# Handle closing brackets
		elif current in [byte_codes.CLOSE_SQUARE, byte_codes.CLOSE_CURLY]:
			if current == byte_codes.CLOSE_SQUARE:
				list_depth -= 1
			if current == byte_codes.CLOSE_CURLY:
				dict_depth -= 1
			if list_depth == 0 and dict_depth == 0:
				return i + 1
			if list_depth == 0 and dict_depth == -1:
				return i

		elif current == byte_codes.COMMA or current == byte_codes.NEWLINE:
			if list_depth == 0 and dict_depth == 0:
				return i
		elif list_depth == 0 and dict_depth == -1:
			return i
		i += 1

	raise TypeError("Invalid JSON")


def count_nesting_in_bytes(json_bytes: bytes, start: int, end: int) -> int:
	"""
	Returns the number of nesting levels between the start and end indices.
	The nesting is counted by the number of opening and closing brackets/braces
	that are not in a string or escaped with a backslash.

	Args:
	- `json_bytes`: A bytes object containing valid JSON when decoded
	"""

	skip_next, in_str, nesting = False, False, 0
	for i in range(start, end):
		if skip_next:
			skip_next = False
			continue
		current = json_bytes[i]
		if current == byte_codes.BACKSLASH:
			skip_next = True
			continue
		if current == byte_codes.QUOTE:
			in_str = not in_str
		if in_str or current == byte_codes.SPACE:
			continue
		elif current == byte_codes.OPEN_CURLY:
			nesting += 1
		elif current == byte_codes.CLOSE_CURLY:
			nesting -= 1
	return nesting


def find_outermost_key_in_json_bytes(json_bytes: bytes, key: str):
	"""
	Returns the index of the key that is at the outermost nesting level. If the
	key is not found, return -1. If the key you are looking for is `some_key`,
	the function will search for `"some_key":` and return the start and end
	index of that string that is at the outermost nesting level, or -1 if the
	it is not found.

	Args:
	- `json_bytes`: A bytes object containing valid JSON when decoded
	- `key`: The key of an key-value pair in `json_bytes` to search for,
	represented as bytes.

	Returns:
	- A tuple of the key start and end index, or `(-1, -1)` if the key is not found.
	"""
	key = f"\"{key}\":".encode()

	if (curr_i := json_bytes.find(key, 0)) == -1:
		return -1, -1

	key_nest = [(curr_i, 0)]  # (key, nesting)

	while (next_i := json_bytes.find(key, curr_i + len(key))) != -1:
		nesting = count_nesting_in_bytes(json_bytes, curr_i + len(key), next_i)
		key_nest.append((next_i, nesting))
		curr_i = next_i

	# Early exit if there is only one key
	if len(key_nest) == 1:
		return key_nest[0][0], key_nest[0][0] + len(key)

	# Relative to total nesting
	for i in range(1, len(key_nest)):
		key_nest[i] = (key_nest[i][0], key_nest[i - 1][1] + key_nest[i][1])

	start_index = min(key_nest, key=lambda x: x[1])[0]
	end_index = start_index + len(key)
	return start_index, end_index


def detect_indentation_in_json_bytes(json_bytes: bytes, index: int) -> Tuple[int, str]:
	"""
	Count the amount of whitespace before the index to determine the indentation
	level and whitespace used.

	Args:
	- `json_bytes`: A bytes object containing valid JSON when decoded
	- `index`: The index behind which the indentation is to be determined

	Returns:
	- A tuple of the indentation level and the whitespace used
	"""

	indentation_bytes, contains_tab = bytes(), False
	for i in range(index - 1, -1, -1):
		if json_bytes[i] not in [byte_codes.SPACE, byte_codes.TAB]:
			break
		if json_bytes[i] == byte_codes.TAB:
			contains_tab = True
		indentation_bytes = indentation_bytes + bytes([json_bytes[i]])

	if contains_tab:
		return len(indentation_bytes), "\t"
	if isinstance(config.indent, int) and config.indent > 0:
		return len(indentation_bytes) // config.indent, " " * config.indent
	if isinstance(config.indent, str):
		return len(indentation_bytes) // 2, "  "
	return 0, ""
