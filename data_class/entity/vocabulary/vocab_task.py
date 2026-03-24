from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabTask:
    id: str
    code: str
    description: Optional[str] = None
