"""Contains DataJoint helper functions."""
from __future__ import annotations

import re
import warnings
from collections.abc import Mapping


def replace_stores(definition: str, stores: Mapping[str, str]) -> str:
    """Replace the store in the definition according to a mapping of replacement to original stores."""
    stores = {original: replacement for replacement, original in stores.items()}

    def replace_store(match):
        try:
            return match.group("prefix") + stores[match.group("original")]
        except KeyError:
            warnings.warn(
                f"No replacement for store '{match.group('original')}' specified. Skipping!", category=UserWarning
            )
            return match.group(0)

    pattern = re.compile(r"(?P<prefix>attach@)(?P<original>\S+)")
    return re.sub(pattern, replace_store, definition)
