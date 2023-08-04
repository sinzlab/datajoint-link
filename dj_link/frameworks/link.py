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
from .mixin import create_mixin


def create_link(
    source_host: str, source_schema: str, local_schema: str, *, stores: Optional[Mapping[str, str]] = None
) -> Callable[[type], Any]:
    """Create a link."""
    if stores is None:
        stores = {}

    def inner(obj: type) -> Any:
        translator = IdentificationTranslator()
        tables = create_tables(DJConfiguration(source_host, source_schema, local_schema, obj.__name__, stores))
        facade = DJLinkFacade(tables.source, tables.outbound, tables.local)
        gateway = DJLinkGateway(facade, translator)
        dj_presenter = DJPresenter()
        handlers = {
            UseCases.PULL: partial(pull, link_gateway=gateway, output_port=dj_presenter.pull),
            UseCases.DELETE: partial(delete, link_gateway=gateway, output_port=dj_presenter.delete),
        }
        controller = DJController(handlers, translator)
        mixin = create_mixin(controller, tables.source, tables.outbound, tables.local)
        return type(obj.__name__, (type(tables.local()), mixin), {})

    return inner
