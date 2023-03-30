"""Contains code gluing the adapters to DataJoint."""
from __future__ import annotations

from ...adapters.datajoint import AbstractTableFacadeLink
from ...base import Base
from .facade import TableFacade


class TableFacadeLink(AbstractTableFacadeLink, Base):
    """Contains the three DataJoint table facades corresponding to the three table types."""

    def __init__(self, source: TableFacade, outbound: TableFacade, local: TableFacade) -> None:
        """Initialize the DataJoint table facade link."""
        self._source = source
        self._outbound = outbound
        self._local = local

    @property
    def source(self) -> TableFacade:
        """Return the source table facade."""
        return self._source

    @property
    def outbound(self) -> TableFacade:
        """Return the outbound table facade."""
        return self._outbound

    @property
    def local(self) -> TableFacade:
        """Return the local table facade."""
        return self._local
