from typing import Tuple

import orjson

from dictdatabase import byte_codes
from dictdatabase import utils
from dictdatabase.dataclasses import SearchResult


def find_key_position_in_bytes(file: bytes, key: str) -> SearchResult:
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
        return SearchResult(start_byte=-1, end_byte=-1, found=False)
    start = key_end + (1 if file[key_end] == byte_codes.SPACE else 0)
    end = utils.seek_index_through_value_bytes(file, start)
    return SearchResult(start_byte=start, end_byte=end, found=True)


def search_key_position_in_db(
    file: bytes, key: str, glom_searching=True
) -> SearchResult:
    original_value_start = 0
    original_value_end = len(file)
    original_key_start = 0
    original_key_end = len(file)
    for k in key.split(".") if glom_searching else [key]:
        key_start, key_end = utils.find_outermost_key_in_json_bytes(file, k)
        if key_end == -1:
            return SearchResult(start_byte=-1, end_byte=-1, found=False)
        original_key_end = original_value_start + key_end
        original_key_start = original_value_start + key_start
        position = find_key_position_in_bytes(file, k)
        original_value_end = original_value_start + original_value_end
        original_value_start += position.start_byte
        file = file[original_value_start:original_value_end]
    return SearchResult(start_byte=original_key_start, end_byte=original_key_end, found=True)


def search_value_position_in_db(
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
        position = find_key_position_in_bytes(
            all_file_bytes[original_start:original_end], k
        )
        if not position.found:
            return -1, -1, False
        original_end = original_start + position.end_byte
        original_start += position.start_byte
    return original_start, original_end, True
