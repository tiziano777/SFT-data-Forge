from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime

@dataclass
class DatasetCard:
    id: Optional[str] = None
    dataset_name: str = ''
    dataset_id: str = ''
    modality: str = ''
    dataset_description: Optional[str] = ''
    publisher: Optional[str] = ''
    notes: Optional[str] = ''
    source_url: Optional[str] = ''
    download_url: Optional[str] = ''
    languages: List[str] = field(default_factory=list)
    license: str = 'unknown'
    core_skills: List[str] = field(default_factory=list)
    tasks: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    source_type: str = ''
    fields: List[str] = field(default_factory=list)
    vertical: List[str] = field(default_factory=list)
    contents: List[str] = field(default_factory=list)
    has_reasoning: bool = False
    last_update: Optional[datetime] = None
    created_at: Optional[datetime] = None
    quality: Optional[int] = 1

    def __post_init__(self):
        # Assicurati che le liste siano sempre liste
        if self.languages is None:
            self.languages = []
        if self.core_skills is None:
            self.core_skills = []
        if self.tasks is None:
            self.tasks = []
        # Assicurati che i campi stringa non siano None
        if self.dataset_description is None:
            self.dataset_description = ''
        if self.publisher is None:
            self.publisher = ''
        if self.notes is None:
            self.notes = ''
        if self.source_url is None:
            self.source_url = ''
        if self.download_url is None:
            self.download_url = ''
        if self.sources is None:
            self.sources = []
        if self.source_type is None:
            self.source_type = ''
        if self.fields is None:
            self.fields = []
        if self.vertical is None:
            self.vertical = []
        if self.contents is None:
            self.contents = []

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "dataset_name": self.dataset_name,
            "dataset_id": self.dataset_id,
            "modality": self.modality,
            "dataset_description": self.dataset_description,
            "publisher": self.publisher,
            "notes": self.notes,
            "source_url": self.source_url,
            "download_url": self.download_url,
            "languages": self.languages,
            "license": self.license,
            "core_skills": self.core_skills,
            "tasks": self.tasks,
            "sources": self.sources,
            "source_type": self.source_type,
            "fields": self.fields,
            "vertical": self.vertical,
            "contents": self.contents,
            "has_reasoning": self.has_reasoning,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "quality": self.quality,
        }