"""A tool for linking two DataJoint tables located on different database servers."""
from .frameworks.datajoint.link import create_link as link
from .schemas import LazySchema

__all__ = ["LazySchema", "link"]
