from typing import Tuple

import orjson

from dictdatabase import byte_codes
from dictdatabase import utils


def find_start_end_in_bytes(file: bytes, key: str) -> Tuple[int, int, bool]:
    """
    It finds the start and end indices of the value of a key in a JSON file

    Args:
      file (bytes): bytes
      key (str): The key to find in the JSON file.

    Returns:
      A tuple of the start and end index of the key, and a boolean value indicating whether the key was found.
    """
    key_start, key_end = utils.find_outermost_key_in_json_bytes(file, key)
    if key_end == -1:
        return -1, -1, False
    start = key_end + (1 if file[key_end] == byte_codes.SPACE else 0)
    end = utils.seek_index_through_value_bytes(file, start)
    return start, end, True


def search_key(file: bytes, key: str, glom_searching=True) -> Tuple[int, int, bool]:
    original_value_start = 0
    original_value_end = len(file)
    original_key_start = 0
    original_key_end = len(file)
    for k in key.split(".") if glom_searching else [key]:
        key_start, key_end = utils.find_outermost_key_in_json_bytes(file, k)
        if key_end == -1:
            return -1, -1, False
        original_key_end = original_value_start + key_end
        original_key_start = original_value_start + key_start
        value_start, value_end, found = find_start_end_in_bytes(file, k)
        original_value_end = original_value_start + original_value_end
        original_value_start += value_start
        file = file[original_value_start:original_value_end]
    return original_key_start, original_key_end, True


def search_value_by_key(
    all_file_bytes: bytes, key: str, glom_searching=True
) -> Tuple[int, int, bool]:
    """
    It takes a byte string, a key, and a boolean, and returns a tuple of three integers

    Args:
      all_file_bytes (bytes): The bytes of the file you're searching in.
      key (str): The key to search for.
      glom_searching: If True, then the key is a glom path, and we need to search for each part of the path. Defaults to
    True

    Returns:
      The start and end of the key in the file.
    """
    original_start = 0
    original_end = len(all_file_bytes)
    for k in key.split(".") if glom_searching else [key]:
        start, end, found = find_start_end_in_bytes(
            all_file_bytes[original_start:original_end], k
        )
        if not found:
            return -1, -1, False
        original_end = original_start + end
        original_start += start
    return original_start, original_end, True
