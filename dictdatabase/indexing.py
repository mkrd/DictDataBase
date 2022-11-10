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
	"""
		The Indexer takes the name of a database file, and tries to load the .index file
		of the corresponding database file.

		The name of the index file is the name of the database file, with the extension
		.index and all "/" replaced with "___"

		The content of the index file is a json object, where the keys are keys inside
		the database json file, and the values are lists of 5 elements:
		- start_index: The index of the first byte of the value of the key in the database file
		- end_index: The index of the last byte of the value of the key in the database file
		- indent_level: The indent level of the key in the database file
		- indent_with: The indent string used.
		- value_hash: The hash of the value bytes
	"""

	invalidate = False

	def __init__(self, db_name: str):
		# Make path of index file
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
		"""
			Returns a list of 5 elements for a key if it exists, otherwise None
			Elements:[start_index, end_index, indent_level, indent_with, value_hash]
		"""
		return self.data.get(key, None)


	def write(self, key, start_index, end_index, indent_level, indent_with, value_hash):
		"""
			Write index information for a key to the index file
		"""

		# TODO:
		# Works when starting from an empty index file
		# But when an index file invalid indices exist, it is not able to get rid of unmatching hashes

		# This seems to be solved by the invalidate flag

		old_key_entry = self.data.get(key, None)
		if old_key_entry is not None:
			_, old_end, _, _, _ = old_key_entry

			# Start should always be the same
			# But end can change if the value length is changed
			if old_end != end_index:
				if not self.invalidate:
					delta = end_index - old_end
					for entry in self.data.values():
						if entry[0] > old_end:
							entry[0] += delta
							entry[1] += delta
				else:
					# Invalidate all keys after the current key
					self.data = {}
					self.invalidate = False

		self.data[key] = [start_index, end_index, indent_level, indent_with, value_hash]
		with open(self.path, "wb") as f:
			f.write(orjson.dumps(self.data))
