from typing import Optional, Callable, Any

storage_directory = "./ddb_storage"
use_compression = False
pretty_json_files = True
custom_json_encoder: Optional[Callable[[dict], str | bytes]] = None
custom_json_decoder: Optional[Callable[[str], dict]] = None
