from dataclasses import dataclass, field

@dataclass
class CardComposition:
    parent_card_name: str
    child_card_name: str
    weight: float = 0.0
