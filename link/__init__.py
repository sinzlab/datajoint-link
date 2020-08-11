from tempfile import TemporaryDirectory

from link.use_cases import initialize
from link.adapters.datajoint.identification import IdentificationTranslator
from link.adapters.datajoint.gateway import DataJointGateway
from link.adapters.datajoint import DataJointGatewayLink
from link.adapters.datajoint.local_table import LocalTablePresenter, LocalTableController
from link.external.datajoint.factory import TableFactory
from link.external.datajoint.facade import TableFacade
from link.external.datajoint.link import Link
from link.schemas import LazySchema


kinds = ("source", "outbound", "local")
table_factories = {kind: TableFactory() for kind in kinds}
Link._table_cls_factories = table_factories
with TemporaryDirectory() as download_path:
    table_facades = {kind: TableFacade(table_factories[kind], download_path) for kind in kinds}
    identification_translators = {kind: IdentificationTranslator(table_facades[kind]) for kind in kinds}
    dj_gateways = {kind: DataJointGateway(table_facades[kind], identification_translators[kind]) for kind in kinds}
    dj_gateway_link = DataJointGatewayLink(**{kind: dj_gateways[kind] for kind in kinds})
    local_table_presenter = LocalTablePresenter()
    pull_use_case = initialize(dj_gateway_link, local_table_presenter.pull)
    local_table_controller = LocalTableController()
    local_table_controller.pull_use_case = pull_use_case
    local_table_controller.source_gateway = dj_gateways["source"]
    Link._local_table_controller = local_table_controller
