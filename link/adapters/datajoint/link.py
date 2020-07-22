class LinkController:
    """Controls the execution of link-related use-cases."""

    def __init__(self, initialize_use_case):
        self.initialize_use_case = initialize_use_case

    def initialize(self, table_name, local_host_name, local_database_name, source_host_name, source_database_name):
        """Initializes all components of the system by calling the "initialize" use-case."""
        self.initialize_use_case(
            table_name, local_host_name, local_database_name, source_host_name, source_database_name
        )

    # TODO: Add __repr__


class LinkPresenter:
    """Presents information about the execution of link-related use-cases to the user."""

    def initialize(self, info):
        """Presents information about the initialization process to the user."""
        # TODO: Transform info to output format
        # TODO: Print transformed info

    # TODO: Add __repr__
