import re
from typing import Dict, Type, List, Optional
from inspect import isclass

from datajoint import Part
from datajoint.table import Table


def replace_stores(definition: str, stores: Dict[str, str]) -> str:
    stores = {original: replacement for replacement, original in stores.items()}

    def replace_store(match):
        return match.groups()[0] + stores[match.groups()[1]]

    pattern = re.compile(r"(attach@)(\S+)")
    return re.sub(pattern, replace_store, definition)


def get_part_table_classes(table_cls: Type[Table], ignored_parts: Optional[List[str]] = None) -> Dict[str, Type[Part]]:
    if ignored_parts is None:
        ignored_parts = []
    part_table_classes = dict()
    for name in dir(table_cls):
        if name[0].isupper() and name not in ignored_parts:
            attr = getattr(table_cls, name)
            if isclass(attr) and issubclass(attr, Part):
                part_table_classes[name] = attr
    return part_table_classes
