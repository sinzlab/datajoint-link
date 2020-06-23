from typing import List

from .repository import Repository
from .address import Address


class SourceRepository(Repository):
    def __init__(self, address: Address, outbound_repo) -> None:
        super().__init__(address)
        self.outbound_repo = outbound_repo

    def delete(self, identifiers: List[str]) -> None:
        for identifier in identifiers:
            if identifier in self.outbound_repo:
                raise RuntimeError(f"Entity with identifier '{identifier}' is in outbound repository")
        super().delete(identifiers)

    def __repr__(self):
        return f"{self.__class__.__qualname__}({repr(self.address)}, {repr(self.outbound_repo)})"
