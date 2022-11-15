from __future__ import annotations



class Confuguration:

	__slots__ = ("storage_directory", "indent", "use_compression", "use_orjson")

	storage_directory: str
	indent: int | str | None  # eg. "\t" or 4 or None
	use_compression: bool
	use_orjson: bool

	def __init__(self, storage_directory="ddb_storage", indent="\t", use_compression=False, use_orjson=True):
		self.storage_directory = storage_directory
		self.indent = indent
		self.use_compression = use_compression
		self.use_orjson = use_orjson



config = Confuguration()
