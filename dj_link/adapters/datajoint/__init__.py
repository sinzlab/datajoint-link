"""Contains code initializing the adapters."""
from abc import ABC, abstractmethod
from typing import Tuple

from ...globals import REPOSITORY_NAMES
from .abstract_facade import AbstractTableFacade
from .gateway import DataJointGateway, DataJointGatewayLink
from .identification import IdentificationTranslator
from .presenter import Presenter, ViewModel


class AbstractTableFacadeLink(ABC):
    """Contains the three DataJoint table facades corresponding to the three table types."""

    @property
    @abstractmethod
    def source(self) -> AbstractTableFacade:
        """Return the table facade corresponding to the source table."""

    @property
    @abstractmethod
    def outbound(self) -> AbstractTableFacade:
        """Return the table facade corresponding to the outbound table."""

    @property
    @abstractmethod
    def local(self) -> AbstractTableFacade:
        """Return the table facade corresponding to the local table."""


def initialize_adapters(
    table_facade_link: AbstractTableFacadeLink,
) -> Tuple[DataJointGatewayLink, ViewModel, Presenter]:
    """Initialize the adapters."""
    translator = IdentificationTranslator()
    gateways = {}
    for repo_type in REPOSITORY_NAMES:
        table_facade = getattr(table_facade_link, repo_type)
        gateways[repo_type] = DataJointGateway(table_facade, translator)
    view_model = ViewModel()
    return DataJointGatewayLink(**gateways), view_model, Presenter(view_model)
