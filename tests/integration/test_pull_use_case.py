from unittest.mock import MagicMock, create_autospec, call

import pytest

from link.entities.abstract_gateway import AbstractEntityDTO
from link.use_cases import AbstractGatewayLink, initialize_use_cases


@pytest.fixture
def identifiers():
    return ["identifier" + str(i) for i in range(10)]


@pytest.fixture
def source_identifiers(identifiers):
    return identifiers


@pytest.fixture
def local_identifiers(identifiers):
    return identifiers[:5]


@pytest.fixture
def requested_identifiers(identifiers):
    return identifiers[2:7]


@pytest.fixture
def pulled_identifiers(identifiers):
    return identifiers[5:7]


@pytest.fixture
def pulled_data(pulled_identifiers):
    return {identifier: create_autospec(AbstractEntityDTO, instance=True) for identifier in pulled_identifiers}


@pytest.fixture
def pulled_outbound_data(pulled_data):
    return {identifier: data.create_identifier_only_copy.return_value for identifier, data in pulled_data.items()}


@pytest.fixture
def gateway_link_spy(source_identifiers, local_identifiers, pulled_data):
    spy = create_autospec(AbstractGatewayLink, instance=True)
    spy.source.identifiers = source_identifiers
    spy.local.identifiers = local_identifiers
    spy.source.fetch.side_effect = pulled_data.values()
    return spy


@pytest.fixture
def pull_output_port_spy():
    return MagicMock(name="pull_output_port_spy")


@pytest.fixture
def pull_use_case(gateway_link_spy, pull_output_port_spy):
    return initialize_use_cases(gateway_link_spy, pull_output_port_spy)["pull"]


@pytest.fixture(autouse=True)
def execute_pull(pull_use_case, requested_identifiers):
    pull_use_case(requested_identifiers)


def test_if_correct_data_is_fetched_from_source_gateway(gateway_link_spy, pulled_identifiers):
    assert gateway_link_spy.source.fetch.call_args_list == [call(identifier) for identifier in pulled_identifiers]


def test_if_correct_data_is_inserted_into_outbound_gateway(gateway_link_spy, pulled_outbound_data):
    assert gateway_link_spy.outbound.insert.call_args_list == [call(data) for data in pulled_outbound_data.values()]


def test_if_correct_data_is_inserted_into_local_gateway(gateway_link_spy, pulled_data):
    assert gateway_link_spy.local.insert.call_args_list == [call(data) for data in pulled_data.values()]
