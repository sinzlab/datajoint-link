class Repository:
    gateway = None
    entity_cls = None

    def __init__(self, address):
        """Initializes Repository."""
        self.address = address
        self._entities = self._create_entities(self.gateway.get_identifiers())
        self._backup = None

    def _create_entities(self, identifiers):
        return {i: self.entity_cls(self.address, i) for i in identifiers}

    @property
    def entities(self):
        return list(self._entities.values())

    def list(self):
        return list(self._entities)

    def fetch(self, identifiers):
        entities = [self._entities[i] for i in identifiers]
        self.gateway.fetch(identifiers)
        return entities

    def delete(self, identifiers):
        working_copy = self._create_entities(self._entities)
        for identifier in identifiers:
            del working_copy[identifier]
        try:
            self.gateway.delete(identifiers)
        except RuntimeError:
            pass
        else:
            self._entities = working_copy

    def insert(self, entities):
        pass

    @property
    def in_transaction(self):
        return bool(self._backup)

    def start_transaction(self):
        if self.in_transaction:
            raise RuntimeError("Can't start transaction while in transaction")
        self.gateway.start_transaction()
        self._backup = self._create_entities(self._entities)

    def commit_transaction(self):
        if not self.in_transaction:
            raise RuntimeError("Can't commit transaction while not in transaction")
        self.gateway.commit_transaction()
        self._backup = None

    def cancel_transaction(self):
        if not self.in_transaction:
            raise RuntimeError("Can't cancel transaction while not in transaction")
        self.gateway.cancel_transaction()
        self._entities = self._create_entities(self._backup)
        self._backup = None

    def __contains__(self, identifier):
        return identifier in self._entities
