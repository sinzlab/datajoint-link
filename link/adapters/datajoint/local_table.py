from typing import Type

from ...base import Base
from .gateway import DataJointGateway
from ...use_cases.refresh import RefreshRequestModel, RefreshUseCase
from ...use_cases.delete import DeleteRequestModel, DeleteUseCase
from ...use_cases.pull import PullRequestModel, PullUseCase


class LocalTableController(Base):
    """Controls the execution of local-table-related use-cases."""

    def __init__(
        self,
        pull_use_case: PullUseCase,
        delete_use_case: DeleteUseCase,
        refresh_use_case: RefreshUseCase,
        pull_request_model_cls: Type[PullRequestModel],
        delete_request_model_cls: Type[DeleteRequestModel],
        refresh_request_model_cls: Type[RefreshRequestModel],
        source_gateway: DataJointGateway,
        local_gateway: DataJointGateway,
    ) -> None:
        self.pull_use_case = pull_use_case
        self.delete_use_case = delete_use_case
        self.refresh_use_case = refresh_use_case
        self.pull_request_model_cls = pull_request_model_cls
        self.delete_request_model_cls = delete_request_model_cls
        self.refresh_request_model_cls = refresh_request_model_cls
        self.source_gateway = source_gateway
        self.local_gateway = local_gateway

    def pull(self, restriction) -> None:
        """Pulls the requested entities from the source table into the local table."""
        identifiers = self.source_gateway.get_identifiers_in_restriction(restriction)
        # noinspection PyArgumentList
        self.pull_use_case(self.pull_request_model_cls(identifiers))

    def delete(self, restriction) -> None:
        """Deletes the requested entities from the local table."""
        identifiers = self.local_gateway.get_identifiers_in_restriction(restriction)
        # noinspection PyArgumentList
        self.delete_use_case(self.delete_request_model_cls(identifiers))

    def refresh(self) -> None:
        """Refreshes the repositories."""
        self.refresh_use_case(self.refresh_request_model_cls())


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
