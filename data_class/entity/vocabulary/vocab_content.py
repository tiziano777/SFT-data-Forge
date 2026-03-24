from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabContent:
    id: str
    code: str
    description: Optional[str] = None
