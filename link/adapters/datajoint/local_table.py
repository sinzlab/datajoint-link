from .gateway import DataJointGateway
from ...use_cases.pull import Pull
from ...use_cases.delete import Delete


class LocalTableController:
    """Controls the execution of local-table-related use-cases."""

    pull_use_case: Pull = None
    delete_use_case: Delete = None
    source_gateway: DataJointGateway = None
    local_gateway: DataJointGateway = None

    def pull(self, restriction) -> None:
        """Pulls the requested entities from the source table into the local table."""
        identifiers = self.source_gateway.get_identifiers_in_restriction(restriction)
        self.pull_use_case(identifiers)

    def delete(self, restriction) -> None:
        """Deletes the requested entities from the local table."""
        identifiers = self.local_gateway.get_identifiers_in_restriction(restriction)
        self.delete_use_case(identifiers)

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"


class LocalTablePresenter:
    """Presents information about the execution of local-table-related use-cases to the user."""

    def pull(self, info):
        """Presents information about the finished pull to the user."""
        # TODO: Transform info to output format
        # TODO: Print transformed info

    def delete(self, info):
        """Presents information about the finished deletion process to the user."""
