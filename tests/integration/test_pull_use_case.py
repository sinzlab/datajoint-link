from unittest.mock import create_autospec, call

import pytest

from link.entities.abstract_gateway import AbstractEntityDTO


USE_CASE = "pull"


@pytest.fixture
def to_be_pulled_identifiers(identifiers):
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
def gateway_link_spy(gateway_link_spy, pulled_data):
    gateway_link_spy.source.fetch.side_effect = pulled_data.values()
    return gateway_link_spy


@pytest.fixture(autouse=True)
def execute_pull(use_case, to_be_pulled_identifiers):
    use_case(to_be_pulled_identifiers)


def test_if_correct_data_is_fetched_from_source_gateway(gateway_link_spy, pulled_identifiers):
    assert gateway_link_spy.source.fetch.call_args_list == [call(identifier) for identifier in pulled_identifiers]


def test_if_correct_data_is_inserted_into_outbound_gateway(gateway_link_spy, pulled_outbound_data):
    assert gateway_link_spy.outbound.insert.call_args_list == [call(data) for data in pulled_outbound_data.values()]


def test_if_correct_data_is_inserted_into_local_gateway(gateway_link_spy, pulled_data):
    assert gateway_link_spy.local.insert.call_args_list == [call(data) for data in pulled_data.values()]
