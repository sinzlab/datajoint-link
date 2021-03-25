"""A tool for linking two DataJoint tables located on different database servers."""
from .adapters.datajoint import initialize_adapters
from .adapters.datajoint.controller import Controller
from .frameworks.datajoint import initialize_frameworks
from .frameworks.datajoint.link import Link, LocalTableMixin  # noqa: F401
from .frameworks.datajoint.printer import Printer
from .schemas import LazySchema  # noqa: F401
from .use_cases import REQUEST_MODELS, USE_CASES, initialize_use_cases

_REPO_NAMES = ("source", "outbound", "local")


def _initialize() -> None:
    facade_link = initialize_frameworks(_REPO_NAMES)
    gateway_link, view_model, presenter = initialize_adapters(facade_link)
    output_ports = {name: getattr(presenter, name) for name in USE_CASES}
    initialized_use_cases = initialize_use_cases(gateway_link, output_ports)
    LocalTableMixin.controller = Controller(initialized_use_cases, REQUEST_MODELS, gateway_link)
    LocalTableMixin.printer = Printer(view_model)


_initialize()
