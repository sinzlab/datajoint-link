"""Contains all commands for the persistence layer."""
from __future__ import annotations

from enum import Enum, auto


class Commands(Enum):
    """Names for all the commands necessary to transition entities between states."""

    ADD_TO_OUTBOUND = auto()
    REMOVE_FROM_OUTBOUND = auto()
    ADD_TO_LOCAL = auto()
    REMOVE_FROM_LOCAL = auto()
    START_PULL_OPERATION = auto()
    FINISH_PULL_OPERATION = auto()
    START_DELETE_OPERATION = auto()
    FINISH_DELETE_OPERATION = auto()
    FLAG = auto()
    UNFLAG = auto()
