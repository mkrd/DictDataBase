from __future__ import annotations

import glob
import os
from typing import Tuple

from . import byte_codes, config


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

	# TODO: Try to implement this using bytes.find() instead of a loop
	# This make count_nesting a lot faster

	# See https://www.json.org/json-en.html for the JSON syntax

	list_depth, dict_depth, i, len_json_bytes = 0, 0, index, len(json_bytes)

	while i < len_json_bytes:
		current = json_bytes[i]
		# If backslash, skip the next character
		if current == byte_codes.BACKSLASH:
			i += 1

		# Assert: the current character is not escaped with a backslash

		elif current == byte_codes.QUOTE:
			while True:
				i = json_bytes.find(byte_codes.QUOTE, i + 1)
				if i == -1:
					raise TypeError("Invalid JSON")

				j = i - 1
				backslash_count = 0
				while j >= 0 and json_bytes[j] == byte_codes.BACKSLASH:
					backslash_count += 1
					j -= 1
				if backslash_count % 2 == 0:
					break

			# Possible exit point where string ends and nesting is zero
			if list_depth == 0 and dict_depth == 0:
				return i + 1

		# Invariant: Not in_str, not escaped

		# Handle opening brackets
		elif current == byte_codes.OPEN_SQUARE:
			list_depth += 1
		elif current == byte_codes.OPEN_CURLY:
			dict_depth += 1
		# Handle closing brackets
		elif current == byte_codes.CLOSE_SQUARE:
			list_depth -= 1
			if list_depth == 0 and dict_depth <= 0:
				return i + 1 + dict_depth  # dict_depth is -1 in case: {"a": {}}
		elif current == byte_codes.CLOSE_CURLY:
			dict_depth -= 1
			if dict_depth <= 0 and list_depth == 0:
				return i + 1 + dict_depth  # dict_depth is -1 in case: {"a": {}}
		elif list_depth == 0:
			if dict_depth == -1:
				return i
			if dict_depth == 0 and current in [byte_codes.COMMA, byte_codes.NEWLINE]:
				# Handle commas and newline as exit points
				return i
		i += 1

	raise TypeError("Invalid JSON")


def count_nesting_in_bytes(json_bytes: bytes, start: int, end: int) -> int:
	"""
	Returns the number of nesting levels.
	Considered bytes are from `start` inclusive to `end` exclusive.

	The nesting is counted by the number of opening and closing brackets/braces
	that are not in a string or escaped with a backslash.

	Args:
	- `json_bytes`: A bytes object containing valid JSON when decoded
	"""
	i, nesting = start, 0
	# Find the number of opening curly braces
	while (i := json_bytes.find(byte_codes.OPEN_CURLY, i, end)) != -1:
		if i == 0 or json_bytes[i - 1] != byte_codes.BACKSLASH:
			nesting += 1
		i += 1
	i = start
	# Find the number of closing curly braces
	while (i := json_bytes.find(byte_codes.CLOSE_CURLY, i, end)) != -1:
		if i == 0 or json_bytes[i - 1] != byte_codes.BACKSLASH:
			nesting -= 1
		i += 1
	return nesting


def find_outermost_key_in_json_bytes(json_bytes: bytes, key: str) -> Tuple[int, int]:
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
	- A tuple of the key start (inclusive) and end (exclusive) index,
	or `(-1, -1)` if the key is not found.
	"""

	# TODO: Very strict. the key must have a colon directly after it
	# For example {"a": 1} will work, but {"a" : 1} will not work!

	key = f"\"{key}\":".encode()

	if (curr_i := json_bytes.find(key, 0)) == -1:
		return (-1, -1)

	# Assert: Key was found and curr_i is the index of the first character of the key

	# Keep track of all found keys and their nesting level
	key_nest = [(curr_i, count_nesting_in_bytes(json_bytes, 0, curr_i))]

	# As long as more keys are found, keep track of them and their nesting level
	while (next_i := json_bytes.find(key, curr_i + len(key))) != -1:
		nesting = count_nesting_in_bytes(json_bytes, curr_i + len(key), next_i)
		key_nest.append((next_i, nesting))
		curr_i = next_i

	# Assert: all keys have been found, and their nesting relative to each other is
	# stored in key_nest, whose length is at least 1.

	# Early exit if there is only one key
	if len(key_nest) == 1:
		index, level = key_nest[0]
		return (index, index + len(key)) if level == 1 else (-1, -1)

	# Relative to total nesting
	for i in range(1, len(key_nest)):
		key_nest[i] = (key_nest[i][0], key_nest[i - 1][1] + key_nest[i][1])

	# Filter out all keys that are not at the outermost nesting level
	indices_at_index_one = [i for i, level in key_nest if level == 1]
	if len(indices_at_index_one) != 1:
		return (-1, -1)
	return (indices_at_index_one[0], indices_at_index_one[0] + len(key))


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
