from dataclasses import dataclass, field
from datetime import datetime, timezone

@dataclass
class Udf:
    id: int
    name: str
    description: str
    function_definition: str
    example_params: list[str]
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))