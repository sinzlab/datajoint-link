from functools import wraps
from contextlib import contextmanager


def _check_identifiers(function):
    @wraps(function)
    def wrapper(self, identifiers):
        for identifier in identifiers:
            if identifier not in self:
                raise KeyError(identifier)
        return function(self, identifiers)

    return wrapper


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
    def identifiers(self):
        return list(self._entities)

    @property
    def entities(self):
        return list(self._entities.values())

    @_check_identifiers
    def fetch(self, identifiers):
        self.gateway.fetch(identifiers)
        entities = [self._entities[i] for i in identifiers]
        return entities

    @_check_identifiers
    def delete(self, identifiers):
        self.gateway.delete(identifiers)
        for identifier in identifiers:
            del self._entities[identifier]

    def insert(self, entities):
        for entity in entities:
            if entity.address != self.address:
                raise ValueError(
                    f"Entity address ('{entity.address}') does not match repository address ('{self.address}')"
                )
            if entity.identifier in self:
                raise ValueError(f"Entity with identifier '{entity.identifier}' is already in repository")
        self.gateway.insert([e.identifier for e in entities])
        for entity in entities:
            self._entities[entity.identifier] = entity

    @property
    def in_transaction(self):
        if self._backup is None:
            return False
        return True

    def start_transaction(self):
        if self.in_transaction:
            raise RuntimeError("Can't start transaction while in transaction")
        self.gateway.start_transaction()
        self._backup = self._create_entities(self.identifiers)

    def commit_transaction(self):
        if not self.in_transaction:
            raise RuntimeError("Can't commit transaction while not in transaction")
        self.gateway.commit_transaction()
        self._backup = None

    def cancel_transaction(self):
        if not self.in_transaction:
            raise RuntimeError("Can't cancel transaction while not in transaction")
        self.gateway.cancel_transaction()
        self._entities = self._create_entities(list(self._backup))
        self._backup = None

    @contextmanager
    def transaction(self):
        self.start_transaction()
        try:
            yield
        except RuntimeError as exception:
            self.cancel_transaction()
            raise exception
        else:
            self.commit_transaction()

    def __contains__(self, identifier):
        return identifier in self.identifiers
