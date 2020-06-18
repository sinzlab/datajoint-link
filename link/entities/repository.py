class Repository:
    gateway = None
    entity_cls = None

    def __init__(self, address):
        """Initializes Repository."""
        self.address = address
        self._entities = self._create_entities(self.gateway.get_identifiers())

    def _create_entities(self, identifiers):
        return {i: self.entity_cls(self.address, i) for i in identifiers}

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

    def __contains__(self, identifier):
        return identifier in self._entities
