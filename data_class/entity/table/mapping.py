from dataclasses import dataclass, field
from typing import Dict, Any
from datetime import datetime, timezone

@dataclass
class Mapping:
    id: str
    serial: int
    distribution_id: str
    schema_template_id: int
    mapping: Dict[str, Any] = None
    version: str = '1.0'
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        if self.mapping is None:
            self.mapping = {}

