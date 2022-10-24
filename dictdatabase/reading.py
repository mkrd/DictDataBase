from path_dict import PathDict
from . import utils, io_safe


def exists(*name) -> bool:
	"""
		Efficiently checks if a database exists.
		If it contains a wildcard, it will return True if at least one exists.
	"""
	return len(utils.find(utils.to_path_str(name))) > 0


def haskey(*name, key: str) -> bool:
	"""
		Checks if a key exists in a database.
		The key can be anywhere in the database, even deeply nested.
		As long it exists as a key in any dict, it will be found.
	"""
	try:
		subread(*name, key=key)
		return True
	except KeyError:
		return False


def read(*name, as_PathDict: bool = False) -> dict | PathDict:
	db = io_safe.read(utils.to_path_str(name))
	return PathDict(db) if as_PathDict else db


def multiread(*pattern, as_PathDict: bool = False):
	"""
		Mutliread reads multiple dbs and returns them as a single dict or PathDict.
		Path components can be "*" (all), a specific name of a list (only those from list).
	"""
	pattern_paths = utils.expand_find_path_pattern(pattern)
	res = {db_name: io_safe.read(db_name) for db_name in pattern_paths}
	return PathDict(res) if as_PathDict else res


def subread(*name, key=None, as_PathDict: bool = False) -> dict | PathDict:
	"""
		Subread reads a database and returns it as a PathDict.
	"""
	path = utils.to_path_str(name)
	_, json_exists, _, ddb_exists = utils.db_paths(path)
	if not json_exists and not ddb_exists:
		return None
	# Wait in any write lock case, "need" or "has".
	data = io_safe.partial_read(path, key)
	return PathDict(data) if as_PathDict else data
