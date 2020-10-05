from ...base import Base
from ...adapters.datajoint.presenter import ViewModel


class Printer(Base):
    """View that uses Python's built-in print function to display information."""

    def __init__(self, view_model: ViewModel) -> None:
        self.view_model = view_model

    def __call__(self) -> None:
        """Formats and prints the data contained in the view model."""
        field_lines = [k + ":" + str(v).rjust(self._width)[len(k) + 1 :] for k, v in self.view_model.fields.items()]
        lines = (
            ["=" * self._width, self.view_model.message.center(self._width), "-" * self._width]
            + field_lines
            + ["=" * self._width]
        )
        print("\n".join(lines))

    @property
    def _width(self) -> int:
        """Computes and returns the width of the printed output."""
        if len(self.view_model.message) >= self._max_field_length:
            return len(self.view_model.message) + 2
        if (self._max_field_length - len(self.view_model.message)) % 2 != 0:
            return self._max_field_length + 1
        return self._max_field_length

    @property
    def _max_field_length(self) -> int:
        """Computes and returns the length of the longest field."""
        return max(len(k + str(v)) + 2 for k, v in self.view_model.fields.items())
