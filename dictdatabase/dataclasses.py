import dataclasses


@dataclasses.dataclass(frozen=True)
class SearchResult:
    start_byte: int
    end_byte: int
    found: bool
