"""Contains code gluing the adapters to DataJoint."""
from typing import Tuple

from ...adapters.datajoint import AbstractTableFacadeLink
from ...base import Base
from .facade import TableFacade
from .factory import TableFactory
from .file import ReusableTemporaryDirectory
from .link import Link
from .mixin import create_local_table_mixin_class


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


def initialize_frameworks(facade_types: Tuple[str, str, str]) -> TableFacadeLink:
    """Initialize the frameworks."""
    temp_dir = ReusableTemporaryDirectory("link_")
    factories = {facade_type: TableFactory() for facade_type in facade_types}
    facades = {facade_type: TableFacade(table_factory, temp_dir) for facade_type, table_factory in factories.items()}

    Link.table_cls_factories = factories

    mixin_class = create_local_table_mixin_class()
    mixin_class.temp_dir = temp_dir
    mixin_class.source_table_factory = factories["source"]
    Link.local_table_mixin = mixin_class
    return TableFacadeLink(**facades)
