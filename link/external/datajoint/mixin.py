from datajoint import AndList

from ...adapters.datajoint.local_table import LocalTableController
from .file import ReusableTemporaryDirectory


class LocalTableMixin:
    """Mixin class for adding additional functionality to the local table class."""

    _controller: LocalTableController = None
    _temp_dir: ReusableTemporaryDirectory = None

    def pull(self, *restrictions) -> None:
        """Pull entities present in the (restricted) source table into the local table."""
        if not restrictions:
            restrictions = AndList()
        with self._temp_dir:
            self._controller.pull(restrictions)
