class LocalTableController:
    """Controls the execution of local-table-related use-cases."""

    pull_use_case = None
    source_gateway = None

    def pull(self, restriction) -> None:
        """Pulls the requested entries from the source_repo_cls table into the local table."""
        identifiers = self.source_gateway.get_identifiers_in_restriction(restriction)
        self.pull_use_case(identifiers)

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"


class LocalTablePresenter:
    """Presents information about the execution of local-table-related use-cases to the user."""

    def pull(self, info):
        """Presents information about the finished pull to the user."""
        # TODO: Transform info to output format
        # TODO: Print transformed info
