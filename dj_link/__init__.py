"""A tool for linking two DataJoint tables located on different database servers."""
from .frameworks.datajoint.link import initialize
from .schemas import LazySchema

__all__ = ["LazySchema"]


Link = initialize()
