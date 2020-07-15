from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class TableEntity:
    master: Dict[str, Any]
    parts: Dict[str, Any]
