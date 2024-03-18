"""Contains preconditions that are applied to the handlers."""
from link.domain.commands import BatchCommand


class NoEntitiesRequested(Exception):
    """This exception is raised when a batch command that requests no entities is encountered."""

    def __init__(self, command: BatchCommand) -> None:
        """Initialize the exception."""
        self.command = command


def requests_entities(command: BatchCommand) -> None:
    """Raise an exception if the given command requests no entities."""
    if not command.requested:
        raise NoEntitiesRequested(command)
