from datajoint import AndList

from ...adapters.datajoint.local_table import LocalTableController
from .file import ReusableTemporaryDirectory
from .factory import TableFactory


class LocalTableMixin:
    """Mixin class for adding additional functionality to the local table class."""

    _controller: LocalTableController = None
    _temp_dir: ReusableTemporaryDirectory = None
    _source_table_factory: TableFactory = None

    def pull(self, *restrictions) -> None:
        """Pull entities present in the (restricted) source table into the local table."""
        if not restrictions:
            restrictions = AndList()
        with self._temp_dir:
            self._controller.pull(restrictions)

    @property
    def source(self):
        """Returns the source table class."""
        return self._source_table_factory()
