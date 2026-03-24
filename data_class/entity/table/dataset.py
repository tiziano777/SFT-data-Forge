from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime, timezone

@dataclass
class Dataset:
    id: Optional[str] = None
    uri: str = ''
    name: str = ''
    languages: List[str] = field(default_factory=list)
    derived_card: Optional[str] = None
    derived_dataset: Optional[str] = None
    dataset_type: Optional[str] = None
    globs: List[str] = field(default_factory=list)
    description: Optional[str] = None
    source: Optional[str] = None
    version: str = '1.0'
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    license: str = 'unknown'
    step: Optional[int] = None

    def __post_init__(self):
        # Assicurati che derived_card sia None se vuoto
        if self.derived_card == "" or self.derived_card == "{}":
            self.derived_card = None
        if self.derived_dataset == "" or self.derived_dataset == "{}":
            self.derived_dataset = None
        
        # Normalizza le liste
        if self.languages is None:
            self.languages = []
        if self.globs is None:
            self.globs = []

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "uri": self.uri,
            "name": self.name,
            "languages": self.languages,
            "derived_card": self.derived_card,
            "derived_dataset": self.derived_dataset,
            "dataset_type": self.dataset_type,
            "globs": self.globs,
            "description": self.description,
            "source": self.source,
            "version": self.version,
            "issued": self.issued,
            "modified": self.modified,
            "license": self.license,
            "step": self.step,
        }