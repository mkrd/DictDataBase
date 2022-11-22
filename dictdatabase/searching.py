from typing import Tuple

from dictdatabase import byte_codes
from dictdatabase import utils


class KeySearcher:
    @staticmethod
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

    def search(
        self, all_file_bytes: bytes, key: str, glom_searching=True
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
            start, end, found = self.find_start_end_in_bytes(
                all_file_bytes[original_start:original_end], k
            )
            if not found:
                return -1, -1, False
            original_end = original_start + end
            original_start += start
        return original_start, original_end, True
