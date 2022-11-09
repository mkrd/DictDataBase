import orjson
import os
from . import config

# Problem: Multiple read processes will concurrently read and write the same file
# In some cases this will result in a empty read error, thats why the try-except exists


# Idea 1:
# - Never write to the index when reading
# - When writing, the lock is exclusive on the index aswell, so no other process can read or write
# Problem: If a file is only ever reed, it will never be indexed

# Idea 2:
# - Write a new index_record to a new unique file
# - Reading index happens from all related files
# - When writing, the new index_record is collected and written into the main file
# Problem: If a file is only ever reed, lots of index record files will accumulate

# Idea 3:
# - Leave everything as is. While not ideal, it works. When empty read error occurs, don't use the index for that read


class Indexer:

	def __init__(self, db_name: str):
		db_name = db_name.replace("/", "___")
		self.path = os.path.join(config.storage_directory, ".ddb", f"{db_name}.index")

		os.makedirs(os.path.dirname(self.path), exist_ok=True)
		if not os.path.exists(self.path):
			self.data = {}
			return

		try:
			with open(self.path, "rb") as f:
				self.data = orjson.loads(f.read())
		except orjson.JSONDecodeError:
			self.data = {}



	def get(self, key):
		return self.data.get(key, None)



	def write(self, key, start_index, end_index, indent_level, indent_with, value_hash):
		self.data[key] = [start_index, end_index, indent_level, indent_with, value_hash]
		with open(self.path, "wb") as f:
			f.write(orjson.dumps(self.data))
