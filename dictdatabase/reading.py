from __future__ import annotations
from path_dict import PathDict
from . import utils, io_safe



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
