from typing import Type

from datajoint import AndList
from datajoint.user_tables import UserTable

from ...adapters.datajoint.local_table import LocalTableController
from .file import ReusableTemporaryDirectory
from .factory import TableFactory


class LocalTableMixin:
    """Mixin class for adding additional functionality to the local table class."""

    _controller: LocalTableController = None
    _temp_dir: ReusableTemporaryDirectory = None
    _source_table_factory: TableFactory = None
    restriction: AndList

    def pull(self, *restrictions) -> None:
        """Pulls entities present in the (restricted) source table into the local table."""
        if not restrictions:
            restrictions = AndList()
        with self._temp_dir:
            self._controller.pull(restrictions)

    def delete(self):
        """Deletes entities from the local table."""
        self._controller.delete(self.restriction)

    def refresh(self):
        """Refreshes the repositories."""
        self._controller.refresh()

    @property
    def source(self) -> Type[UserTable]:
        """Returns the source table class."""
        return self._source_table_factory()
