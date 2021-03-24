"""A tool for linking two DataJoint tables located on different database servers."""
from typing import Dict

from dj_link.adapters.datajoint import DataJointGatewayLink, initialize_adapters
from dj_link.adapters.datajoint.controller import Controller
from dj_link.adapters.datajoint.presenter import Presenter, ViewModel
from dj_link.frameworks.datajoint import TableFacadeLink
from dj_link.frameworks.datajoint.facade import TableFacade
from dj_link.frameworks.datajoint.factory import TableFactory
from dj_link.frameworks.datajoint.file import ReusableTemporaryDirectory
from dj_link.frameworks.datajoint.link import Link, LocalTableMixin
from dj_link.frameworks.datajoint.printer import Printer
from dj_link.schemas import LazySchema  # noqa: F401
from dj_link.use_cases import REQUEST_MODELS, initialize_use_cases

_REPO_NAMES = ("source", "outbound", "local")


def _initialize() -> None:
    factories = {n: TableFactory() for n in _REPO_NAMES}
    Link.table_cls_factories = factories
    temp_dir = ReusableTemporaryDirectory("link_")
    facade_link = TableFacadeLink(**{n: TableFacade(factories[n], temp_dir) for n in _REPO_NAMES})
    gateway_link, view_model, presenter = initialize_adapters(facade_link)
    _configure_local_table_mixin(gateway_link, presenter, temp_dir, factories, view_model)


def _configure_local_table_mixin(
    gateway_link: DataJointGatewayLink,
    presenter: Presenter,
    temp_dir: ReusableTemporaryDirectory,
    factories: Dict[str, TableFactory],
    view_model: ViewModel,
) -> None:
    initialized_use_cases = initialize_use_cases(
        gateway_link,
        dict(
            pull=presenter.pull,
            delete=presenter.delete,
            refresh=presenter.refresh,
        ),
    )
    LocalTableMixin.controller = Controller(initialized_use_cases, REQUEST_MODELS, gateway_link)
    LocalTableMixin.temp_dir = temp_dir
    LocalTableMixin.source_table_factory = factories["source"]
    LocalTableMixin.printer = Printer(view_model)


_initialize()
