from link.external.datajoint.link import Link
from link.schemas import LazySchema


def initialize():
    from link.use_cases import initialize_use_cases
    from link.adapters.datajoint.identification import IdentificationTranslator
    from link.adapters.datajoint.gateway import DataJointGateway
    from link.adapters.datajoint import DataJointGatewayLink
    from link.adapters.datajoint.local_table import LocalTablePresenter, LocalTableController
    from link.external.datajoint.file import ReusableTemporaryDirectory
    from link.external.datajoint.factory import TableFactory
    from link.external.datajoint.facade import TableFacade
    from link.external.datajoint.link import LocalTableMixin

    kinds = ("source", "outbound", "local")
    table_factories = {kind: TableFactory() for kind in kinds}
    Link._table_cls_factories = table_factories
    temp_dir = ReusableTemporaryDirectory("link_")
    table_facades = {kind: TableFacade(table_factories[kind], temp_dir) for kind in kinds}
    identification_translators = {kind: IdentificationTranslator(table_facades[kind]) for kind in kinds}
    dj_gateways = {kind: DataJointGateway(table_facades[kind], identification_translators[kind]) for kind in kinds}
    dj_gateway_link = DataJointGatewayLink(**{kind: dj_gateways[kind] for kind in kinds})
    local_table_presenter = LocalTablePresenter()
    initialized_use_cases = initialize_use_cases(
        dj_gateway_link,
        dict(
            pull=local_table_presenter.pull, delete=local_table_presenter.delete, refresh=local_table_presenter.refresh
        ),
    )
    local_table_controller = LocalTableController(
        initialized_use_cases["pull"], initialized_use_cases["delete"], dj_gateways["source"], dj_gateways["local"]
    )
    LocalTableMixin._controller = local_table_controller
    LocalTableMixin._temp_dir = temp_dir
    LocalTableMixin._source_table_factory = table_factories["source"]


initialize()
