class LinkController:
    """Controls the execution of link-related use-cases."""

    initialize_use_case = None

    def __init__(self, local_schema, source_schema, stores=None):
        self.local_schema = local_schema
        self.source_schema = source_schema
        self.stores = stores
        self.table_cls = None

    def __call__(self, table_cls):
        """Initializes all components of the system by calling the "initialize" use-case."""
        self.table_cls = table_cls
        self.initialize_use_case(
            table_cls.__name__,
            self.local_schema.host,
            self.local_schema.database,
            self.source_schema.host,
            self.source_schema.database,
        )


class LinkPresenter:
    """Presents information about the execution of link-related use-cases to the user."""

    def initialize(self, info):
        """Presents information about the initialization process to the user."""
        # TODO: Transform info to output format
        # TODO: Print transformed info
