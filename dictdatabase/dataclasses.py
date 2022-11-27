import dataclasses


@dataclasses.dataclass(frozen=True)
class SearchResult:
    start_byte: int
    end_byte: int
    found: bool


@dataclasses.dataclass(frozen=True)
class Index:
    key: str
    key_start: int
    key_end: int
    indent_level: int
    indent_with: str
    value_hash: str
    old_value_end: int
