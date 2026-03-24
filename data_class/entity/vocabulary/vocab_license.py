from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabLicense:
    id: str
    code: str
    description: Optional[str] = None
    license_url: Optional[str] = None
    note: Optional[str] = None



