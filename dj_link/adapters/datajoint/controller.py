from typing import Type, Mapping, TypedDict

from ...base import Base
from .gateway import DataJointGateway
from ...use_cases.base import AbstractUseCase
from ...use_cases.pull import PullRequestModel
from ...use_cases.delete import DeleteRequestModel
from ...use_cases.refresh import RefreshRequestModel


class RequestModelClasses(TypedDict):
    pull: Type[PullRequestModel]
    delete: Type[DeleteRequestModel]
    refresh: Type[RefreshRequestModel]


class Controller(Base):
    """Controls the execution of use-cases at the user's request."""

    def __init__(
        self,
        use_cases: Mapping[str, AbstractUseCase],
        request_model_classes: RequestModelClasses,
        gateways: Mapping[str, DataJointGateway],
    ) -> None:
        self.use_cases = use_cases
        self.request_model_classes = request_model_classes
        self.gateways = gateways

    def pull(self, restriction) -> None:
        """Pulls the requested entities from the source table into the local table."""
        identifiers = self.gateways["source"].get_identifiers_in_restriction(restriction)
        # noinspection PyArgumentList
        self.use_cases["pull"](self.request_model_classes["pull"](identifiers))

    def delete(self, restriction) -> None:
        """Deletes the requested entities from the local table."""
        identifiers = self.gateways["local"].get_identifiers_in_restriction(restriction)
        # noinspection PyArgumentList
        self.use_cases["delete"](self.request_model_classes["delete"](identifiers))

    def refresh(self) -> None:
        """Refreshes the repositories."""
        self.use_cases["refresh"](self.request_model_classes["refresh"]())


class LocalTablePresenter:
    """Presents information about the execution of local-table-related use-cases to the user."""

    def pull(self, info):
        """Presents information about the finished pull to the user."""
        # TODO: Transform info to output format
        # TODO: Print transformed info

    def delete(self, info):
        """Presents information about the finished deletion process to the user."""

    def refresh(self, info):
        """Presents information about the finished refresh process to the user."""
