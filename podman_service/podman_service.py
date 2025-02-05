from cyclopts import App, Parameter
from podman_actions import (
    run_container,
    transform_bindings,
    attach_networks_to_container,
)
import uuid
from typing import Annotated
from pathlib import Path
from typing import Optional
from loguru import logger


def get_base_path() -> Path:
    """Returns the base path of the project code. The 'src' folder"""
    return Path(__file__).parents[1] / "src"


def get_data_folder(id: Optional[str] = None) -> Path:
    """Get the data folder for services to write"""
    if not id:
        return Path(__file__).parents[1] / "data"
    return Path(__file__).parents[1] / "data" / "containers_data" / f"{id}"


def get_basic_bindings() -> list[tuple[Path, Path]]:
    base_path = get_base_path()
    basic_bindings = [
        (
            base_path / "service_discovery",
            Path("/app/code") / "service_discovery",
        ),
        (
            base_path / "logging_helper",
            Path("/app/code") / "logging_helper",
        ),
        (
            base_path / "file_helpers",
            Path("/app/code") / "file_helpers",
        ),
        (
            base_path / "async_sockets",
            Path("/app/code") / "async_sockets",
        ),
    ]
    return basic_bindings


app = App(
    help="A CLI app for interacting with the podman service and for easily creation of container with bindings",
    version="0.1.0",
)

client_app = App(
    name="client", help="A CLI app for interacting with the server file system"
)
app.command(client_app)


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


@app.command()
def create_async_server_example():
    # TODO: Implement
    image_name = "distributed_python:basic"
    base_path = get_base_path()
    id = uuid.uuid4().hex
    container_name: str = f"async_sock_server_{id}"
    bindings = [
        *get_basic_bindings(),
        (
            base_path / "async_sockets_examples",
            Path("/app/code") / "async_sockets_examples",
        ),
        (
            get_data_folder(container_name),
            Path("/app/data/"),
        ),
    ]
    bindings = transform_bindings(bindings)
    env_vars = {
        "DATA_FOLDER": "/app/data",
    }
    container = run_container(
        container_name=container_name,
        image_name=image_name,
        bindings=bindings,
        environment=env_vars,
        network="servers",
        command=[
            "python",
            "./code/async_sockets_examples/file_server.py",
            "start-server",
        ],
        # command=["python", "./code/main.py"],
    )
    print(f"Container {container.name} created")


@app.command()
def create_async_client_example(
    to_ip: str,
    to_port: int,
    file_name: str,
):
    # TODO: Implement
    image_name = "distributed_python:basic"
    base_path = get_base_path()
    id = uuid.uuid4().hex
    container_name: str = f"async_sock_client_{id}"

    bindings = [
        *get_basic_bindings(),
        (
            base_path / "async_sockets_examples",
            Path("/app/code") / "async_sockets_examples",
        ),
        (
            get_data_folder(container_name),
            Path("/app/data"),
        ),
        (
            get_data_folder() / "files_to_send",
            Path("/app/files_to_send"),
        ),
    ]
    bindings = transform_bindings(bindings)
    env_vars = {
        "DATA_FOLDER": "/app",
    }
    container = run_container(
        container_name=container_name,
        image_name=image_name,
        bindings=bindings,
        network="servers",
        environment=env_vars,
        command=[
            # "sudo",
            "python",
            "./code/async_sockets_examples/async_client.py",
            "send-file",
            to_ip,
            str(to_port),
            file_name,
        ],
        # command=["fish"],
        # command=["python", "./code/main.py"],
    )
    print(f"Container {container.name} created")


@app.command()
def create_storage_server():
    """Creates the server for the tag based filesystem"""
    image_name = "distributed_python:basic"
    base_path = get_base_path()
    id = uuid.uuid4().hex
    container_name: str = f"storage_server_{id}"
    bindings = [
        *get_basic_bindings(),
        (
            get_data_folder(container_name),
            Path("/app/data"),
        ),
        (
            base_path / "chord_subsystem",
            Path("/app/code") / "chord_subsystem",
        ),
        (
            base_path / "server",
            Path("/app/code") / "server",
        ),
    ]
    bindings = transform_bindings(bindings)
    env_vars = {
        "DATA_FOLDER": "/app/data",
    }
    container = run_container(
        container_name=container_name,
        image_name=image_name,
        bindings=bindings,
        network="servers",
        environment=env_vars,
        command=[
            "python",
            "/app/code/server/server_service.py",
            "start-server",
        ],
        # command=["python", "./code/main.py"],
    )
    print(f"Container {container.name} created")


# @app.command()
def create_client(
    args: list[str],
):
    """Creates the client for the tag based filesystem"""
    image_name = "distributed_python:basic"
    base_path = get_base_path()
    id = uuid.uuid4().hex
    container_name: str = f"client_{id}"
    bindings = [
        *get_basic_bindings(),
        (
            get_data_folder(container_name),
            Path("/app/data"),
        ),
        (
            base_path / "client_cli",
            Path("/app/code") / "client_cli",
        ),
        (
            get_data_folder() / "files_to_send",
            Path("/app/files_to_send"),
        ),
    ]
    bindings = transform_bindings(bindings)
    env_vars = {
        "DATA_FOLDER": "/app/data",
    }
    container = run_container(
        container_name=container_name,
        image_name=image_name,
        bindings=bindings,
        network="servers",
        environment=env_vars,
        command=[
            "python",
            "/app/code/client_cli/cli_app.py",
            *args,
        ],
        # command=["python", "./code/main.py"],
    )
    print(f"Container {container.name} created")


@client_app.command(name="add-files")
def client_add_files(
    file_names: Annotated[
        list[str],
        Parameter(
            name="files",
            consume_multiple=True,
        ),
    ],
    tags: Annotated[
        list[str],
        Parameter(
            consume_multiple=True,
        ),
    ],
):
    """
    Adds files to the server annotated with the tags
    """
    logger.info(f"Adding files ({file_names}) with tags ({tags})")
    create_client(
        [
            "add-files",
            "--files",
            *file_names,
            "--tags",
            *tags,
        ]
    )


@client_app.command(name="download-files")
def client_download_files(
    file_names: Annotated[
        list[str],
        Parameter(
            name="--files",
            consume_multiple=True,
        ),
    ],
):
    """
    Downloads files from the server
    """
    create_client(
        [
            "download-files",
            "--files",
            *file_names,
        ]
    )


@client_app.command()
def client_list_files(
    tag_query: Annotated[
        list[str],
        Parameter(
            name="--tag-query",
            consume_multiple=True,
        ),
    ],
):
    """
    Lists files from the server based on the tags
    """
    create_client(
        [
            "list-files",
            "--tag-query",
            *tag_query,
        ]
    )


@client_app.command()
def client_delete_files(
    tag_query: Annotated[
        list[str],
        Parameter(
            name="--tag-query",
            consume_multiple=True,
        ),
    ],
):
    """
    Deletes files from the server
    """
    create_client(
        [
            "delete-files",
            "--tag-query",
            *tag_query,
        ]
    )


@client_app.command()
def add_tags(
    tag_query: Annotated[
        list[str],
        Parameter(
            name="--tag-query",
            consume_multiple=True,
        ),
    ],
    tag_list: Annotated[
        list[str],
        Parameter(
            name="--tag-list",
            consume_multiple=True,
        ),
    ],
):
    """Adds tags to files based on the tag query"""
    create_client(
        [
            "add-tags",
            "--tag-query",
            *tag_query,
            "--tag-list" * tag_list,
        ]
    )


@client_app.command()
def delete_tags(
    tag_query: Annotated[
        list[str],
        Parameter(
            name="--tag-query",
            consume_multiple=True,
        ),
    ],
    tag_list: Annotated[
        list[str],
        Parameter(
            name="--tag-list",
            consume_multiple=True,
        ),
    ],
):
    """Deletes tags from files based on the tag query"""
    create_client(
        [
            "delete-tags",
            "--tag-query",
            *tag_query,
            "--tag-list",
            *tag_list,
        ]
    )


if __name__ == "__main__":
    app()
