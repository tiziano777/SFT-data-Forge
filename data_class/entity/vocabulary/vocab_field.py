from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabField:
    id: str
    code: str
    description: Optional[str] = None
