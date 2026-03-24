from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabVertical:
    id: str
    code: str
    description: Optional[str] = None
