"""A tool for linking two DataJoint tables located on different database servers."""
from .frameworks.datajoint.link import Link, initialize
from .schemas import LazySchema

__all__ = ["Link", "LazySchema"]


initialize()
