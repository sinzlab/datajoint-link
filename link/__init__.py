"""A tool for linking two DataJoint tables located on different database servers."""
from .infrastructure.link import create_link as link

__all__ = ["link"]
