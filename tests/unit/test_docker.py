"""Contains unit tests for the ContainerRunner class."""
from contextlib import AbstractContextManager
from functools import partial
from unittest.mock import MagicMock, call

import pytest

from dj_link.docker import ContainerRunner


def test_if_subclass_of_abstract_context_manager():
    assert issubclass(ContainerRunner, AbstractContextManager)


@pytest.fixture()
def container_spy():
    container_spy = MagicMock(name="container_spy")
    container_spy.name = "container_spy"

    def side_effect():
        if side_effect.n_calls < 5:
            attrs = {"State": {"Health": {"Status": "starting"}}}
        else:
            attrs = {"State": {"Health": {"Status": "healthy"}}}
        container_spy.attrs = attrs
        side_effect.n_calls += 1

    side_effect.n_calls = 0
    container_spy.reload.side_effect = side_effect
    return container_spy


@pytest.fixture()
def docker_client_spy(container_spy):
    client = MagicMock(name="docker_client_spy")
    client.containers.run.return_value = container_spy
    client.__repr__ = MagicMock(name="docker_client_spy.__repr__", return_value="docker_client_spy")
    return client


@pytest.fixture()
def container_config():
    return {"image": "my-image"}


@pytest.fixture()
def container_runner(docker_client_spy, container_config):
    return partial(ContainerRunner, docker_client_spy, container_config)


def test_if_docker_client_is_stored_as_instance_attribute(docker_client_spy, container_runner):
    assert container_runner().docker_client is docker_client_spy


def test_if_container_config_is_stored_as_instance_attribute(container_config, container_runner):
    assert container_runner().container_config == container_config


def test_if_value_error_is_raised_if_detach_is_false(docker_client_spy, container_config):
    container_config["detach"] = False
    with pytest.raises(ValueError, match="'detach' must be 'True' or omitted"):
        ContainerRunner(docker_client_spy, container_config)


def test_if_health_check_config_is_stored_as_instance_attribute(container_runner):
    health_check_config = {"max_retries": 40, "interval": 2}
    assert container_runner(health_check_config=health_check_config).health_check_config == health_check_config


def test_if_default_health_check_config_is_used_if_health_check_config_not_provided(container_runner):
    assert container_runner().health_check_config == ContainerRunner.default_health_check_config


def test_if_remove_is_stored_as_instance_attribute(container_runner):
    assert container_runner(remove=False).remove is False


def test_if_remove_is_true_by_default(container_runner):
    assert container_runner().remove is True


def test_if_runtime_error_is_raised_when_accessing_container_attribute_outside_of_context(container_runner):
    with pytest.raises(RuntimeError, match="Container not running"):
        _ = container_runner().container


def test_if_container_is_stored_as_instance_attribute(container_runner):
    container_runner.container = "container"
    assert container_runner.container == "container"


def test_if_run_method_of_docker_client_is_called_correctly(docker_client_spy, container_runner):
    with container_runner(health_check_config={"interval": 0}):
        docker_client_spy.containers.run.assert_called_once_with(image="my-image")


def test_if_reload_method_of_container_is_called_correctly(container_spy, container_runner):
    with container_runner(health_check_config={"interval": 0}):
        assert container_spy.reload.call_args_list == [call()] * 6


def test_if_container_is_stopped_if_not_healthy_after_max_retries(container_runner, container_spy):
    with pytest.raises(RuntimeError):
        with container_runner(health_check_config={"max_retries": 5, "interval": 0}):
            container_spy.stop.assert_called_once_with()


def test_if_runtime_error_is_raised_if_not_healthy_after_max_retries(container_runner, container_spy):
    with pytest.raises(RuntimeError) as exc_info:
        with container_runner(health_check_config={"max_retries": 5, "interval": 0}):
            assert exc_info.value.args[0] == "Container 'container' not healthy after max number (2) of retries"


def test_if_container_is_returned(container_runner, container_spy):
    with container_runner(health_check_config={"interval": 0}) as returned:
        assert returned is container_spy


def test_if_container_is_stopped(container_runner, container_spy):
    with container_runner(health_check_config={"interval": 0}):
        pass
    container_spy.stop.assert_called_once_with()


def test_if_container_is_removed_if_remove_is_true(container_runner, container_spy):
    with container_runner(health_check_config={"interval": 0}, remove=True):
        pass
    container_spy.remove.assert_called_once_with(v=True)


def test_if_container_is_not_removed_if_remove_is_false(container_runner, container_spy):
    with container_runner(health_check_config={"interval": 0}, remove=False):
        pass
    container_spy.remove.assert_not_called()


def test_repr(container_runner, docker_client_spy, container_config):
    assert repr(container_runner()) == (
        f"ContainerRunner(docker_client={docker_client_spy}, container_config={container_config}, "
        "health_check_config={'max_retries': 60, 'interval': 1.0}, remove=True)"
    )
