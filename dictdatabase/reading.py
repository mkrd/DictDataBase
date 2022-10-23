from path_dict import PathDict
from . import utils, io_safe, io_unsafe


def exists(*name) -> bool:
	"""
		Efficiently checks if a database exists.
		If it contains a wildcard, it will return True if at least one exists.
	"""
	return len(utils.find(utils.to_path_str(name))) > 0


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
	pf = io_unsafe.partial_read(utils.to_path_str(name), key)
	return PathDict(pf.key_value) if as_PathDict else pf.key_value
