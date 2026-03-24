from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabDatasetType:
    id: str
    code: str
    description: Optional[str] = None
