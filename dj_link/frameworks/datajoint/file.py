from contextlib import AbstractContextManager
from tempfile import TemporaryDirectory

from ...base import Base


class ReusableTemporaryDirectory(AbstractContextManager, Base):
    """A reusable version of Python's TemporaryDirectory class."""

    temp_dir_cls = TemporaryDirectory

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self._temp_dir: TemporaryDirectory

    def __enter__(self) -> str:
        """Returns the name of a newly created temporary directory."""
        self._temp_dir = self.temp_dir_cls(prefix=self.prefix)
        return self.name

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Cleans up the temporary directory created by entering the with clause."""
        self._temp_dir.cleanup()
        del self._temp_dir

    @property
    def name(self) -> str:
        return self._temp_dir.name
