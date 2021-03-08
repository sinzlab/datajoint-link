"""Contains a context manager for running Docker containers."""
import time
from contextlib import AbstractContextManager
from typing import Any, Dict, Mapping, Optional, TypedDict

from docker import DockerClient
from docker.models.containers import Container


class HealthCheckConfig(TypedDict):
    """Health check configuration for ContainerRunner."""

    max_retries: int
    interval: float


class ContainerRunner(AbstractContextManager):
    """Context manager for running Docker containers."""

    default_health_check_config: HealthCheckConfig = {"max_retries": 60, "interval": 1.0}

    def __init__(
        self,
        docker_client: DockerClient,
        container_config: Mapping[str, Any],
        health_check_config: Optional[HealthCheckConfig] = None,
        remove: bool = True,
    ) -> None:
        """Initialize ContainerRunner.

        The "detach" key must be either omitted from the container configuration or its value must be "True".
        """
        self.docker_client = docker_client
        self.container_config = self._process_container_config(container_config)
        self.health_check_config = self._process_health_check_config(health_check_config)
        self.remove = remove
        self._container: Optional[Container] = None

    @staticmethod
    def _process_container_config(container_config: Mapping[str, Any]) -> Dict[str, Any]:
        if not container_config.get("detach", True):
            raise ValueError("'detach' must be 'True' or omitted")
        return dict(container_config)

    def _process_health_check_config(self, health_check_config: Optional[HealthCheckConfig]):
        if health_check_config is None:
            return self.default_health_check_config
        return {**self.default_health_check_config, **health_check_config}

    @property
    def container(self):
        """Return the container if present, otherwise raise an error."""
        if self._container is None:
            raise RuntimeError("Container not running")
        return self._container

    @container.setter
    def container(self, container):
        self._container = container

    def __enter__(self) -> Container:
        """Return the healthy container."""
        self._run_container()
        self._wait_until_healthy()
        return self.container

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Tear down the container."""
        self.container.stop()
        if self.remove:
            self.container.remove(v=True)

    def _run_container(self) -> None:
        self.container = self.docker_client.containers.run(**self.container_config)

    def _wait_until_healthy(self) -> None:
        for _ in range(self.health_check_config["max_retries"]):
            if self._is_healthy:
                break
            time.sleep(self.health_check_config["interval"])
        else:
            self._abort()

    @property
    def _is_healthy(self) -> bool:
        self.container.reload()
        return self.container.attrs["State"]["Health"]["Status"] == "healthy"

    def _abort(self) -> None:
        self.container.stop()
        raise RuntimeError(
            (
                f"Container '{self.container.name}' not healthy after max number "
                f"({self.health_check_config['max_retries']}) of retries"
            )
        )

    def __repr__(self) -> str:
        """Return a string representation of the object."""
        return (
            f"{self.__class__.__name__}(docker_client={self.docker_client}, "
            f"container_config={self.container_config}, health_check_config={self.health_check_config}, "
            f"remove={self.remove})"
        )
