from cyclopts import App, Parameter
from podman_actions import (
    run_container,
    transform_bindings,
    attach_networks_to_container,
)
import uuid
from typing import Annotated
from pathlib import Path


def get_base_path() -> Path:
    """Returns the base path of the project code. The 'src' folder"""
    return Path(__file__).parents[1] / "src"


app = App(
    help="A CLI app for interacting with the podman service and for easily creation of container with bindings",
    version="0.1.0",
)


@app.command(name="create-discovery-test-node")
def create_discovery_test_node():
    """Create a node that will test the discovery service"""
    image_name = "distributed_python:basic"
    base_path = get_base_path()
    bindings = [
        (
            base_path / "service_discovery",
            Path("/app/code") / "service_discovery",
        ),
        (
            base_path / "logging_helper",
            Path("/app/code") / "logging_helper",
        ),
        (
            base_path / "tag_based_volume_system",
            Path("/app/data"),
        ),
    ]
    bindings = transform_bindings(bindings)
    id = uuid.uuid4().hex
    container = run_container(
        container_name=f"zmq_discovery_{id}",
        image_name=image_name,
        bindings=bindings,
        network="servers",
        command=["python", "./code/service_discovery/example.py"],
        # command=["python", "./code/main.py"],
    )
    print(f"Container {container.name} created")


if __name__ == "__main__":
    app()
