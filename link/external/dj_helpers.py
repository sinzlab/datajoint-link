import re
from typing import Dict


def replace_stores(definition: str, stores: Dict[str, str]) -> str:
    def replace_store(match):
        return match.groups()[0] + stores[match.groups()[1]]

    pattern = re.compile(r"(attach@)(\S+)")
    return re.sub(pattern, replace_store, definition)
