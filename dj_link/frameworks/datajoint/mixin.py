"""Contains mixins that add functionality to DataJoint tables."""
from typing import Type

from datajoint import AndList
from datajoint.user_tables import UserTable

from ...adapters.datajoint.controller import Controller
from .factory import TableFactory
from .file import ReusableTemporaryDirectory
from .printer import Printer


class LocalTableMixin:
    """Mixin class for adding additional functionality to the local table class."""

    controller: Controller
    temp_dir: ReusableTemporaryDirectory
    source_table_factory: TableFactory
    printer: Printer
    restriction: AndList

    def pull(self, *restrictions) -> None:
        """Pull entities present in the (restricted) source table into the local table."""
        if not restrictions:
            restrictions = AndList()
        with self.temp_dir:
            self.controller.pull(restrictions)
        self.printer()

    def delete(self):
        """Delete entities from the local table."""
        self.controller.delete(self.restriction)
        self.printer()

    def refresh(self):
        """Refresh the repositories."""
        self.controller.refresh()
        self.printer()

    @property
    def source(self) -> Type[UserTable]:
        """Return the source table class."""
        return self.source_table_factory()
