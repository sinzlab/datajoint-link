"""Contains logic related to input/output handling to/from services."""
from __future__ import annotations

from functools import partial
from typing import Any, Callable, Generic, Iterable, Protocol, TypeVar

from .services import Request, Response

_Response = TypeVar("_Response", bound=Response)


class ResponseRelay(Generic[_Response]):
    """A relay that makes the response of one service available to another."""

    def __init__(self) -> None:
        """Initialize the relay."""
        self._response: _Response | None = None

    def get_response(self) -> _Response:
        """Return the response of the relayed service."""
        assert self._response is not None
        return self._response

    def __call__(self, response: _Response) -> None:
        """Store the response of the relayed service."""
        self._response = response


_Request = TypeVar("_Request", bound=Request)


def create_returning_service(
    service: Callable[[_Request], None], get_response: Callable[[], _Response]
) -> Callable[[_Request], _Response]:
    """Create a version of the provided service that returns its response when executed."""

    def execute(request: _Request) -> _Response:
        service(request)
        return get_response()

    return execute


def create_response_forwarder(recipients: Iterable[Callable[[_Response], None]]) -> Callable[[_Response], None]:
    """Create an object that forwards the response it gets called with to multiple recipients."""
    recipients = list(recipients)

    def duplicate_response(response: _Response) -> None:
        for recipient in recipients:
            recipient(response)

    return duplicate_response


_Response_co = TypeVar("_Response_co", bound=Response, covariant=True)

_Request_contra = TypeVar("_Request_contra", bound=Request, contravariant=True)


class Service(Protocol[_Request_contra, _Response_co]):
    """Protocol for services."""

    def __call__(self, request: _Request_contra, *, output_port: Callable[[_Response_co], None], **kwargs: Any) -> None:
        """Execute the service."""


class ReturningService(Protocol[_Request_contra, _Response_co]):
    """Protocol for services that return their response."""

    def __call__(
        self, request: _Request_contra, *, output_port: Callable[[_Response_co], None], **kwargs: Any
    ) -> _Response_co:
        """Execute the service."""


def make_responsive(service: Service[_Request, _Response]) -> ReturningService[_Request, _Response]:
    """Create a version of the service that returns its response in addition to sending it to the output port."""
    relay: ResponseRelay[_Response] = ResponseRelay()
    service = partial(service, output_port=relay)

    def returning_service(request: _Request, *, output_port: Callable[[_Response], None], **kwargs: Any) -> _Response:
        service(request, **kwargs)
        output_port(relay.get_response())
        return relay.get_response()

    return returning_service
