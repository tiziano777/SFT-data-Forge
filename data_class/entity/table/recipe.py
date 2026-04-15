from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime, timezone

@dataclass
class Recipe:
    id: Optional[str] = None
    name: str = ""
    description: Optional[str] = ""
    scope: str = "sft"
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: str = "1.0.0"
    tasks: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    derived_from : Optional[str] = None

    def to_downloadable_dict(self, entries: dict) -> dict:
        """
        Converts the Recipe instance into a downloadable dictionary format.
        Adds metadata (recipe_id, name, description) and nests entries under 'entries'.
        """
        return {
            "recipe_id": self.id,
            "name": self.name,
            "description": self.description,
            "entries": entries
        }