from typing import Callable, Generic, Iterable, Optional

import pytest

from dj_link.entities.custom_types import Identifier
from dj_link.use_cases import RepositoryLinkFactory
from dj_link.use_cases.base import ResponseModel
from dj_link.use_cases.gateway import GatewayLink
from dj_link.use_cases.pull import PullRequestModel, PullResponseModel, PullUseCase
from tests.unit.entities.assignments import create_identifiers

from ..conftest import FakeGatewayLinkCreator


class FakeOutputPort(Generic[ResponseModel]):
    """Remembers the last response that was passed to it."""

    def __init__(self) -> None:
        self.response: Optional[ResponseModel] = None

    def __call__(self, response: ResponseModel) -> None:
        self.response = response


@pytest.fixture()
def fake_output_port() -> FakeOutputPort[PullResponseModel]:
    return FakeOutputPort()


def pull(
    gateway_link: GatewayLink, output_port: Callable[[PullResponseModel], None], *, requested: Iterable[Identifier]
) -> None:
    use_case = PullUseCase(gateway_link, RepositoryLinkFactory(gateway_link), output_port)
    request = PullRequestModel(list(requested))
    use_case(request)


def test_correct_entities_are_pulled(
    create_fake_gateway_link: FakeGatewayLinkCreator,
    fake_output_port: FakeOutputPort[PullResponseModel],
) -> None:
    fake_gateway_link = create_fake_gateway_link(
        {
            "source": create_identifiers("1", "2", "3"),
            "outbound": create_identifiers("2"),
            "local": create_identifiers("2"),
        }
    )
    pull(fake_gateway_link, fake_output_port, requested=create_identifiers("1", "2"))
    assert set(fake_gateway_link.outbound) == set(fake_gateway_link.local) == create_identifiers("1", "2")


def test_correct_response_model_is_sent_to_output_port(
    create_fake_gateway_link: FakeGatewayLinkCreator, fake_output_port: FakeOutputPort[PullResponseModel]
) -> None:
    fake_gateway_link = create_fake_gateway_link(
        {
            "source": create_identifiers("1", "2", "3"),
            "outbound": create_identifiers("2"),
            "local": create_identifiers("2"),
        }
    )
    pull(fake_gateway_link, fake_output_port, requested=create_identifiers("1", "2"))
    assert fake_output_port.response == PullResponseModel(
        requested=create_identifiers("1", "2"), valid=create_identifiers("1"), invalid=create_identifiers("2")
    )
