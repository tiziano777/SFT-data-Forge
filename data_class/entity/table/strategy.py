from dataclasses import dataclass, field
from sys import version
from typing import Optional
from datetime import datetime, timezone

@dataclass
class Strategy:
    id: Optional[str] = None
    recipe_id: str = ""
    distribution_id: str = ""
    replication_factor: int = 1
    template_strategy: Optional[str] = None
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))