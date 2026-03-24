from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

@dataclass
class SystemPrompt:
    id: int
    name: str
    description: str
    prompt: str
    length: int
    _lang: str = None
    derived_from: Optional[str] = None
    quality_score: float = 0.0
    deleted: bool = False
    version: str = '1.0'
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
