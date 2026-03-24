from dataclasses import dataclass, field
from datetime import datetime, timezone

from click import prompt
from duckdb import description
from shapely import length

@dataclass
class SystemPromptHistory:
    id: str
    system_prompt_id: int
    name: str
    description: str
    prompt: str
    length: int
    version: str
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))