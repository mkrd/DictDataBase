import glob
from .objects import DDBSession, DDBMultiSession, PathDict
from . import utils
from . import config
from .objects import SubModel
from .locking import find_locks

from path_dict import PathDict



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
	return [f for r in res for f in find("/".join(r))]



def _to_path_if_tuple(s):
	if isinstance(s, tuple) and len(s) > 0:
		return "/".join(list(s))
	return s



def find(*pattern):
	"""
		Find multiple dbs with a glob pattern,
		and return their names as a list.
	"""
	pattern = _to_path_if_tuple(pattern)
	dbs_ddb = glob.glob(f"{config.storage_directory}/{pattern}.ddb")
	dbs_json = glob.glob(f"{config.storage_directory}/{pattern}.json")
	dbs_all = dbs_ddb + dbs_json
	for trim in [f"{config.storage_directory}/", ".ddb", ".json"]:
		dbs_all = [d.replace(trim, "") for d in dbs_all]
	return dbs_all



def exists(*pattern):
	"""
		Tells if a glob pattern finds a db.
	"""
	pattern = _to_path_if_tuple(pattern)
	return len(find(pattern)) > 0



def read(*name, as_PathDict: bool = False):
	name = _to_path_if_tuple(name)

	if len(find_locks("haswrite", name)) > 0:
		raise Exception("Never access the same db again during an open session!")

	db = utils.protected_read_json_as_dict(name)
	if as_PathDict:
		return PathDict(db)
	return db


def multiread(*pattern, as_PathDict: bool = False):
	"""
		Mutliread reads multiple dbs and returns them as a single dict or PathDict.
		Path components can be "*" (all), a specific name or a list (only those from list).
	"""
	pattern_paths = expand_find_path_pattern(pattern)
	res = {}
	for db_name in pattern_paths:
		res[db_name] = utils.protected_read_json_as_dict(db_name)
	if as_PathDict:
		return PathDict(res)
	return res



def create(*db_name, db={}):
	db_name = _to_path_if_tuple(db_name)
	if isinstance(db, PathDict):
		utils.protected_write_dict_as_json(db_name, db.dict)
	else:
		utils.protected_write_dict_as_json(db_name, db)



def delete(*db_name):
	db_name = _to_path_if_tuple(db_name)
	utils.protected_delete(db_name)


def session(*db_name, as_PathDict: bool = False):
	db_name = _to_path_if_tuple(db_name)
	return DDBSession(db_name, as_PathDict=as_PathDict)

def multisession(*pattern, as_PathDict: bool = False):
	"""
		Open multiple files at once using a glob pattern, like "user*".
		Mutliple arguments are allowed to access folders,
		so multisession(f"users/{user_id}") is equivalent
		to multisession("users", user_id).
	"""
	pattern = _to_path_if_tuple(pattern)
	return DDBMultiSession(pattern, as_PathDict=as_PathDict)




class Model(PathDict):
	"""
		Used to collect functions of a particular db file or
		set of files
	"""
	def __init__(self, *db_name):
		if not exists(*db_name):
			return None
		db_name = _to_path_if_tuple(db_name)
		self.db_name = db_name
		self.reread()


	def reread(self):
		self.data = utils.protected_read_json_as_dict(self.db_name)

	def write(self):
		utils.protected_write_dict_as_json(self.db_name, self.data)