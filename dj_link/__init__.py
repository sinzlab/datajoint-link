from dj_link.frameworks.datajoint.link import Link
from dj_link.schemas import LazySchema


_REPO_NAMES = ("source", "outbound", "local")


def _initialize():
    from dj_link.adapters.datajoint.identification import IdentificationTranslator
    from dj_link.adapters.datajoint.gateway import DataJointGateway
    from dj_link.adapters.datajoint.presenter import Presenter, ViewModel
    from dj_link.frameworks.datajoint.file import ReusableTemporaryDirectory
    from dj_link.frameworks.datajoint.factory import TableFactory
    from dj_link.frameworks.datajoint.facade import TableFacade

    factories = {n: TableFactory() for n in _REPO_NAMES}
    Link._table_cls_factories = factories
    temp_dir = ReusableTemporaryDirectory("link_")
    facades = {n: TableFacade(factories[n], temp_dir) for n in _REPO_NAMES}
    translators = {n: IdentificationTranslator(facades[n]) for n in _REPO_NAMES}
    gateways = {n: DataJointGateway(facades[n], translators[n]) for n in _REPO_NAMES}
    view_model = ViewModel()
    presenter = Presenter(translators, view_model)
    _configure_local_table_mixin(gateways, presenter, temp_dir, factories, view_model)


def _configure_local_table_mixin(gateways, presenter, temp_dir, factories, view_model):
    from dj_link.use_cases import REQUEST_MODELS, initialize_use_cases
    from dj_link.adapters.datajoint import DataJointGatewayLink
    from dj_link.adapters.datajoint.controller import Controller
    from dj_link.frameworks.datajoint.link import LocalTableMixin
    from dj_link.frameworks.datajoint.printer import Printer

    LocalTableMixin._controller = Controller(
        initialize_use_cases(
            DataJointGatewayLink(**{n: gateways[n] for n in _REPO_NAMES}),
            dict(
                pull=presenter.pull,
                delete=presenter.delete,
                refresh=presenter.refresh,
            ),
        ),
        REQUEST_MODELS,
        gateways,
    )
    LocalTableMixin._temp_dir = temp_dir
    LocalTableMixin._source_table_factory = factories["source"]
    LocalTableMixin._printer = Printer(view_model)


_initialize()
