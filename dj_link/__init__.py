"""A tool for linking two DataJoint tables located on different database servers."""
from .frameworks.datajoint.link import link as Link
from .schemas import LazySchema

__all__ = ["LazySchema", "Link"]
