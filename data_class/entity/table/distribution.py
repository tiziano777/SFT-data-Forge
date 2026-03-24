from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

@dataclass
class Distribution:
    id: str
    uri: str
    dataset_id: str
    glob: str
    format: str
    name: str
    tokenized_uri: Optional[str] = None
    query: Optional[str] = None
    script: Optional[str] = None
    lang: str = 'un'
    split: Optional[str] = None
    derived_from: Optional[str] = None
    src_schema: Dict[str, Any] = None
    description: Optional[str] = None
    tags: List[str] = None
    license: str = 'unknown'
    version: str = '1.0'
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    materialized: bool = True
    step: Optional[int] = None

    def __post_init__(self):
        # Assicura che derived_from sia None se vuoto o non valido
        if self.derived_from in ["", "{}", {}]:
            self.derived_from = None
            
        if self.src_schema in [{}, None, "", "{}"]:
            self.src_schema = {}
            
        if self.tags in [None, "", "{}"]:
            self.tags = []
    
    def copy(self):
        """
        Create a copy of the current Distribution instance.
        Returns:
            Distribution: A new instance of Distribution with the same attributes.
        """
        return Distribution(
            id=self.id,
            uri=self.uri,
            dataset_id=self.dataset_id,
            glob=self.glob,
            format=self.format,
            name=self.name,
            query=self.query,
            script=self.script,
            lang=self.lang,
            split=self.split,
            derived_from=self.derived_from,
            src_schema=self.src_schema.copy() if self.src_schema else {},
            description=self.description,
            tags=self.tags.copy() if self.tags else [],
            license=self.license,
            version=self.version,
            issued=self.issued,
            modified=self.modified,
            materialized=self.materialized,
            step=self.step
        )

