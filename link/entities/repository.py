from typing import Optional, Dict
from dataclasses import dataclass, field


@dataclass
class Entity:
    identifier: str
    flags: Optional[Dict[str, bool]] = field(default_factory=dict)
