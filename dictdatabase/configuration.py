from __future__ import annotations
from dataclasses import dataclass



@dataclass
class Confuguration:
	storage_directory: str = "ddb_storage"
	indent: int | str | None = "\t"  # eg. "\t" or 4 or None
	use_compression: bool = False
	use_orjson: bool = True



config = Confuguration()
