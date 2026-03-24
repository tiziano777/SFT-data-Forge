from dataclasses import dataclass, field
from typing import Dict, Any
from datetime import datetime, timezone

@dataclass
class MappingHistory:
    id: str
    mapping_id: int
    schema_template_id: int
    mapping: Dict[str, Any]
    version: str
    modified_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
