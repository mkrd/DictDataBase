from . import utils


class DBFileMeta:
	__slots__ = ("path", "exists", "json_path", "json_exists", "ddb_path", "ddb_exists")

	def __init__(self, name: str):
		json_path, json_exists, ddb_path, ddb_exists = utils.file_info(name)
		self.path = name
		self.json_path = json_path
		self.json_exists = json_exists
		self.ddb_path = ddb_path
		self.ddb_exists = ddb_exists
		self.exists = json_exists or ddb_exists
