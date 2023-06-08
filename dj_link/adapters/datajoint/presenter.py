"""Contains the presenter class and related classes/functions."""
from dataclasses import dataclass
from typing import Dict, Optional, TypedDict

from ...base import Base
from ...use_cases.delete import DeleteResponseModel
from ...use_cases.pull import PullResponseModel
from ...use_cases.refresh import RefreshResponseModel
from .identification import UUIDIdentificationTranslator


@dataclass
class ViewModel:
    """Contains data used by the view."""

    _message: Optional[str] = None
    _fields: Optional[Dict[str, int]] = None

    def update(self, message: str, fields: Dict[str, int]) -> None:
        """Update the view model."""
        self._message = message
        self._fields = fields

    @property
    def message(self) -> str:
        """Return the message or raise an error if it not set."""
        if self._message is None:
            raise RuntimeError("Message attribute not set")
        return self._message

    @property
    def fields(self) -> Dict[str, int]:
        """Return the fields or raise an error if they are not set."""
        if self._fields is None:
            raise RuntimeError("Fields attribute not set")
        return self._fields


class Translators(TypedDict):
    """Contains one identification translator for each of the three table types."""

    source: UUIDIdentificationTranslator
    outbound: UUIDIdentificationTranslator
    local: UUIDIdentificationTranslator


class Presenter(Base):
    """Updates the view model based on the information present in the response model."""

    def __init__(self, view_model: ViewModel) -> None:
        """Initialize the presenter."""
        self.view_model = view_model

    def pull(self, response_model: PullResponseModel) -> None:
        """Update the view model based on information present in the response model of the pull use-case."""
        self.view_model.update(
            "Pull was successful",
            {
                "Number of requested entities": response_model.n_requested,
                "Number of valid entities": response_model.n_valid,
                "Number of invalid entities": response_model.n_invalid,
            },
        )

    def delete(self, response_model: DeleteResponseModel) -> None:
        """Update the view model based on information present in the response model of the delete use-case."""
        self.view_model.update(
            "Deletion was successful",
            {
                "Number of requested entities": response_model.n_requested,
                "Number of entities that had their deletion approved": response_model.n_deletion_approved,
                "Number of entities that were deleted from outbound table": response_model.n_deleted_from_outbound,
                "Number of entities that were deleted from local table": response_model.n_deleted_from_local,
            },
        )

    def refresh(self, response_model: RefreshResponseModel) -> None:
        """Update the view model based on information present in the response model of the refresh use-case."""
        self.view_model.update("Refresh was successful", {"Number of refreshed entities": response_model.n_refreshed})
