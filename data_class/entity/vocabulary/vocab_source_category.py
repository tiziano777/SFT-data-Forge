from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabSourceCategory:
    id: str
    code: str
    description: Optional[str] = None
