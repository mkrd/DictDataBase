import hashlib

from dictdatabase import utils
from dictdatabase.dataclasses import Index


def create_index(all_file_bytes: bytes, key: str, start, end) -> Index:
	"""
	It takes a JSON file, a key, and a start and end position, and returns a tuple of information about the key and its
	value

	Args:
		all_file_bytes (bytes): The entire file as a byte string.
		key (str): The key of the value we're indexing.
		start: the start of the value in the file
		end: the end of the value in the file

	Returns:
		The key, start, end, indent_level, indent_with, value_hash, end
	"""
	key_start, key_end = utils.find_outermost_key_in_json_bytes(all_file_bytes, key)
	indent_level, indent_with = utils.detect_indentation_in_json_bytes(
		all_file_bytes, key_start
	)
	value_bytes = all_file_bytes[start:end]
	value_hash = hashlib.sha256(value_bytes).hexdigest()
	return Index(key, start, end, indent_level, indent_with, value_hash, end)
