from time import sleep
from podman import PodmanClient
from podman.domain.networks import Network
from podman.domain.images import Image
from podman.domain.containers import Container
from podman.domain.volumes import Volume, VolumesManager
from rich import print, print_json
from rich.text import Text
import json
from pathlib import Path
from typing import Any
import time
import os


def get_uri():
    """Returns the URI that allows the connection to podman service"""
    return "unix:///run/user/1000/podman/podman.sock"


def get_user_group() -> int:
    return os.getgid()


def get_mounts(bindings: dict[str, str]) -> list[dict]:
    """Returns a list of mounts based on the bindings dictionary.
    The key of the dictionary is the container path and the value is the host path."""
    mounts = []
    default_mount = {
        "type": "bind",
        "readonly": False,
        "bind-propagation": "rshared",
        # "relabel": "Z",
        # "relabel": "z",
        "relabel": "shared",
        # "idmap": True,
    }
    for container_path, host_path in bindings.items():
        mount = default_mount.copy()
        mount["source"] = host_path
        mount["target"] = container_path
        mounts.append(mount)
    return mounts


def transform_bindings(bindings: list[tuple[Path, Path]]) -> list[dict[str, Any]]:
    """Returns the bindings in the format requested by podman"""
    mounts = []
    default_mount = {
        "type": "bind",
        "read_only": False,
        # "relabel": "Z",
        "relabel": "z",
        # "chown": True,
        # "relabel": "no",
        "BindOptions": {
            "Propagation": "rshared",
            "NonRecursive": False,
        },  # Propagación recursiva y compartida
        # "mode": "rw",
        # "security_opt":["label:disable"],  # Deshabilitar SELinux
    }
    for host_path, container_path in bindings:
        host_path.mkdir(parents=True, exist_ok=True)
        # container_path.mkdir(parents=True, exist_ok=True)
        mount = default_mount.copy()
        mount["source"] = str(host_path)
        mount["target"] = str(container_path)
        mounts.append(mount)
    return mounts


# Let's get a network by name
def get_network_by_name(network_name: str) -> Network:
    """Get a network by name"""
    with PodmanClient(base_url=get_uri()) as client:
        network = client.networks.get(network_name)
        # print_network_info(network)
        return network


def print_network_info(network: Network):
    """
    Print the information of the network as a Json file
    """
    print("=" * 20 + f" Network '{network.name}' " + "=" * 20 + "\n")
    info = {
        "network_name": network.name,
        "network_id": network.id,
        # "network_driver": network.attrs["Driver"],
        # "network_scope": network.attrs["Scope"],
        # "network_info": network.attrs,
        "other_info": network.attrs,
    }
    print_json(json.dumps(info, indent=4))
    print("=" * 50 + "\n")


def print_container_info(container: Container):
    """
    Prints the container info as Json
    """
    # container info
    header = "=" * 20 + f" Container '{container.name}' " + "=" * 20 + "\n"
    print(f"{header:<60}")
    info = {
        "container_name": container.name,
        "container_id": container.id,
        "container_ip": container.attrs["NetworkSettings"]["IPAddress"],
        "container_network_mode": container.attrs["HostConfig"]["NetworkMode"],
        "container_published_ports": container.attrs["NetworkSettings"]["Ports"],
        "container_status": container.status,
        "container_image_name": container.attrs["Config"]["Image"],
        # "container_networks": container.attrs["NetworkSettings"]["Networks"],
        "container_network_settings": container.attrs["NetworkSettings"],
    }
    print_json(json.dumps(info, indent=4))
    print()
    # available fields
    print(sorted(container.attrs.keys()))
    print("=" * 70 + "\n")


def attach_networks_to_container(
    container: str | Container,
    networks: list[str],
):
    try:
        if isinstance(container, str):
            container = client.containers.get(container)
    except Exception as e:
        error = Text(repr(e))
        print(e)
    """Attach the container to all the networks specified"""
    with PodmanClient(base_url=get_uri()) as client:
        if len(networks) > 0:
            net = client.networks.get("podman")
            net.disconnect(container)
        for network_name in networks:
            try:
                net = client.networks.get(network_name)
                net.connect(container)
                time.sleep(1)
            except Exception as e:
                error = Text(repr(e.with_traceback()), style="red")
                print(error)


# def attach_volume_to_container(
#     container_name: str,
#     volumes: list[tuple[Path, Path]],
# ):
#     with PodmanClient(base_url=get_uri()) as client:
#         try:
#             container = client.containers.get(container_name)
#         except Exception as e:
#             print(e.with_traceback())


def run_container(
    container_name: str,
    image_name: str,
    bindings: list[dict] = [],
    command: str = None,
    network: str = "podman",
    environment: list[str] | dict[str, str] = [],
) -> Container:
    """Run a container with a given name and image name"""
    with PodmanClient(base_url=get_uri()) as client:
        try:
            existing_container = client.containers.get(container_name)
            # existing_container.stop()
            existing_container.remove(force=True)
            client.containers.prune()
            print(f"Removed existing container: {container_name}")
            sleep(4)
        except Exception as e:
            # pass  # El contenedor no existe, continuar
            pass
            # print(f"Container '{container_name}' does not exist")
        # if networks == []:
        #     networks = ["my-network"]
        # network = get_network_by_name("my-network")
        group_id = get_user_group()
        container = client.containers.run(
            name=container_name,
            image=image_name,
            detach=True,
            mounts=bindings,
            network_mode="bridge",
            environment=environment,
            group_add=[str(group_id)],
            command=command,
            network=network,  # This works
            # networks=networks, # Doesn't work
            tty=True,
            security_opt=["disable:True"],  # Deshabilitar SELinux
            userns_mode="keep-id",  # This is the important line for allowing the write of files in non-root containers
        )
        # net = get_network_by_name(networks[0])
        # net.connect(container)
        time.sleep(1)
        return container
        # print_container_info(container)
