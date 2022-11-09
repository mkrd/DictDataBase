import orjson
import os
from . import config



class Indexer:

	def __init__(self, db_name: str):
		db_name = db_name.replace("/", "___")
		self.path = os.path.join(config.storage_directory, ".ddb", f"{db_name}.index")

		os.makedirs(os.path.dirname(self.path), exist_ok=True)
		if not os.path.exists(self.path):
			self.data = {}
		else:
			while True:
				try:

					with open(self.path, "rb") as f:
						fr = f.read()

						frl = orjson.loads(fr)

						self.data = frl
						return
				except:
					print("❌ except")



	def get(self, key):
		return self.data.get(key, None)



	def write(self, key, start_index, end_index, indent_level, indent_with, value_hash):
		self.data[key] = [start_index, end_index, indent_level, indent_with, value_hash]
		with open(self.path, "wb") as f:
			f.write(orjson.dumps(self.data))
