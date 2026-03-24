from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabSourceType:
    id: str
    code: str
    description: Optional[str] = None
