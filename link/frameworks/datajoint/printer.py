from ...base import Base
from ...adapters.datajoint.presenter import ViewModel


class Printer(Base):
    def __init__(self, view_model: ViewModel) -> None:
        self.view_model = view_model

    def __call__(self) -> None:
        width = self._compute_width()
        field_lines = [k + ":" + str(v).rjust(width)[len(k) + 1 :] for k, v in self.view_model.fields.items()]
        lines = ["=" * width, self.view_model.message.center(width), "-" * width] + field_lines + ["=" * width]
        print("\n".join(lines))

    def _compute_width(self) -> int:
        message_width = len(self.view_model.message)
        max_field_width = max(len(k + str(v)) + 2 for k, v in self.view_model.fields.items())
        if message_width >= max_field_width:
            return message_width + 2
        if (max_field_width - message_width) % 2 != 0:
            return max_field_width + 1
        return max_field_width
