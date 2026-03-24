from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime, timezone

@dataclass
class SchemaTemplate:
    id: str
    serial: int
    name: str
    schema: Dict[str, Any]
    description: Optional[str] = None
    version: str = '1.0'
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

