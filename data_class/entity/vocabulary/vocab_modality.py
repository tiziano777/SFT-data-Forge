from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabModality:
    id: str
    code: str
    description: Optional[str] = None
    mime: Optional[list[str]] = None

