from typing import Any, List


def _represent(instance: Any, attrs: List[str]) -> str:
    """Creates a string representing the provided instance."""
    attr_reprs = (name + "=" + repr(getattr(instance, name)) for name in attrs)
    return instance.__class__.__qualname__ + "(" + ", ".join(attr_reprs) + ")"
