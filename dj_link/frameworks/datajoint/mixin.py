from typing import Type

from datajoint import AndList
from datajoint.user_tables import UserTable

from ...adapters.datajoint.controller import Controller
from .file import ReusableTemporaryDirectory
from .factory import TableFactory
from .printer import Printer


class LocalTableMixin:
    """Mixin class for adding additional functionality to the local table class."""

    _controller: Controller
    _temp_dir: ReusableTemporaryDirectory
    _source_table_factory: TableFactory
    _printer: Printer
    restriction: AndList

    def pull(self, *restrictions) -> None:
        """Pulls entities present in the (restricted) source table into the local table."""
        if not restrictions:
            restrictions = AndList()
        with self._temp_dir:
            self._controller.pull(restrictions)
        self._printer()

    def delete(self):
        """Deletes entities from the local table."""
        self._controller.delete(self.restriction)
        self._printer()

    def refresh(self):
        """Refreshes the repositories."""
        self._controller.refresh()
        self._printer()

    @property
    def source(self) -> Type[UserTable]:
        """Returns the source table class."""
        return self._source_table_factory()
