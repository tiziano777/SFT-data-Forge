from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabDistributionSplit:
    id: str
    code: str
    description: Optional[str] = None
