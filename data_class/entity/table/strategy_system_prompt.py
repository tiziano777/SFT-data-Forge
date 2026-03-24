from dataclasses import dataclass
from typing import Optional

@dataclass
class StrategySystemPrompt:
    strategy_id: Optional[str] = None
    system_prompt_name: Optional[str] = None