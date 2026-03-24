from dataclasses import dataclass
from typing import Optional


@dataclass
class VocabLanguage:
    id: str
    code: str
    description: Optional[str] = None
    