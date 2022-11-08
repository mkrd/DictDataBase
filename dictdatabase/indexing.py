import orjson
from pathlib import Path
import os
from . import config


class Indexer:
	def __init__(self, db_name: str):
		self.path = f"{config.storage_directory}/.ddb/{db_name.replace('/', '___')}.index"
		Path(self.path).parent.mkdir(parents=True, exist_ok=True)
		if not os.path.exists(self.path):
			self.data = {}
		else:
			with open(self.path, "rb") as f:
				self.data = orjson.loads(f.read())

	def get(self, key):
		return self.data.get(key, None)

	def write(self, key, start_index, end_index, indent_level, indent_with, value_hash):
		self.data[key] = [start_index, end_index, indent_level, indent_with, value_hash]
		with open(self.path, "wb") as f:
			f.write(orjson.dumps(self.data))
