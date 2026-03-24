from dataclasses import dataclass, field
from typing import Optional, Any, Dict
from datetime import datetime, timezone

@dataclass
class Checkpoint:
    id: Optional[str] = None
    recipe_id: str = ""
    checkpoint_number: int = 1
    src_uri: str = ""
    name: str = ""
    description: Optional[str] = ""
    results: Dict[str, Any] = field(default_factory=dict)
    hyperparams: Dict[str, Any] = field(default_factory=dict)
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))