from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabCoreSkill:
    id: str
    code: str
    description: Optional[str] = None

