from typing import Dict, Any, List


class DataStorage:
    def __init__(self) -> None:
        self._storage = dict()

    def store(self, data: Dict[str, Any]) -> None:
        self._storage.update(data)

    def retrieve(self, identifiers: List[str]) -> Dict[str, Any]:
        return {identifier: self._storage[identifier] for identifier in identifiers}

    def __contains__(self, identifier) -> bool:
        return identifier in self._storage

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"
