"""Contains the link decorator that is used by the user to establish a link."""
from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import Any, Mapping, Optional

from dj_link.adapters.controller import DJController
from dj_link.adapters.gateway import DJLinkGateway
from dj_link.adapters.identification import IdentificationTranslator
from dj_link.adapters.presenter import DJPresenter
from dj_link.use_cases.use_cases import UseCases, delete, pull

from . import DJConfiguration, create_tables
from .facade import DJLinkFacade
from .mixin import create_local_endpoint


def create_link(  # noqa: PLR0913
    source_host: str,
    source_schema: str,
    outbound_schema: str,
    outbound_table: str,
    local_schema: str,
    *,
    stores: Optional[Mapping[str, str]] = None,
) -> Callable[[type], Any]:
    """Create a link."""
    if stores is None:
        stores = {}

    def inner(obj: type) -> Any:
        translator = IdentificationTranslator()
        tables = create_tables(
            DJConfiguration(
                source_host, source_schema, outbound_schema, outbound_table, local_schema, obj.__name__, stores
            )
        )
        facade = DJLinkFacade(tables.source, tables.outbound, tables.local)
        gateway = DJLinkGateway(facade, translator)
        dj_presenter = DJPresenter()
        handlers = {
            UseCases.PULL: partial(pull, link_gateway=gateway, output_port=dj_presenter.pull),
            UseCases.DELETE: partial(delete, link_gateway=gateway, output_port=dj_presenter.delete),
        }
        controller = DJController(handlers, translator)
        return create_local_endpoint(controller, tables)

    return inner
