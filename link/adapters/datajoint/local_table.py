from ...base import Base
from .gateway import DataJointGateway
from ...use_cases.base import UseCase


class Controller(Base):
    """Controls the execution of use-cases at the user's request."""

    def __init__(
        self,
        pull_use_case: UseCase,
        delete_use_case: UseCase,
        refresh_use_case: UseCase,
        source_gateway: DataJointGateway,
        local_gateway: DataJointGateway,
    ) -> None:
        self.pull_use_case = pull_use_case
        self.delete_use_case = delete_use_case
        self.refresh_use_case = refresh_use_case
        self.source_gateway = source_gateway
        self.local_gateway = local_gateway

    def pull(self, restriction) -> None:
        """Pulls the requested entities from the source table into the local table."""
        identifiers = self.source_gateway.get_identifiers_in_restriction(restriction)
        self.pull_use_case(identifiers)

    def delete(self, restriction) -> None:
        """Deletes the requested entities from the local table."""
        identifiers = self.local_gateway.get_identifiers_in_restriction(restriction)
        self.delete_use_case(identifiers)

    def refresh(self) -> None:
        """Refreshes the repositories."""
        self.refresh_use_case()


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
