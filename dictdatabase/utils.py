import os
import glob
from typing import Tuple
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


def expand_find_path_pattern(pattern):
	"""
		:param str pattern: The pattern to expand.
		Fot a tuple of path items, expand it to a list of all real paths.
		An item can be some string, a wildcard "*" or a list to select specific paths.
	"""
	res = [[]]
	for item in pattern:
		if isinstance(item, str):
			res = [r + [item] for r in res]
		if isinstance(item, list):
			res = [r + [list_item] for list_item in item for r in res]
	return [f for r in res for f in find(r)]
