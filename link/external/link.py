class Link:
    """User interface used to link a new DataJoint table to a pre-existing one."""

    controller = None

    def __init__(self, local_schema, source_schema, stores=None):
        """Initializes Link."""
        self.local_schema = local_schema
        self.source_schema = source_schema
        self.stores = stores
        self.table_cls = None

    def __call__(self, table_cls):
        """Creates a new DataJoint table and links it to a pre-existing one located in the source schema."""
        self.controller.initialize(
            table_cls.__name__,
            self.local_schema.host,
            self.local_schema.database,
            self.source_schema.host,
            self.source_schema.database,
        )
        return self.table_cls
