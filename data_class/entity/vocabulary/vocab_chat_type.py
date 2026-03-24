from dataclasses import dataclass
from typing import Optional

@dataclass
class VocabChatType:
    id: str
    code: str
    schema_id: str
    description: Optional[str] = None
    


