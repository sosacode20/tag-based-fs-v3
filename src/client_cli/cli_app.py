from basic_imports import *
from cyclopts import App, Parameter
from loguru import logger
from pathlib import Path
from typing import Optional, Annotated
from rich import print
from async_sockets.async_connection import AsyncConnection
from file_helpers.file_helper import FileHelper, FileMetadata
from logging_helper.logging_utils import LogFilters, custom_log_format
import os
from service_discovery.service_watcher import ServiceWatcher, Peer
from client_cli.commands.constants import Commands, CLIENT_SYSTEM, SERVER_SERVICE_NAME
from client_cli.commands.send_file import send_files
import asyncio
import random as rnd


def get_data_path() -> Path:
    """Returns the Path where the data should be stored"""
    folder_path = os.getenv("DATA_FOLDER")
    if folder_path:
        return Path(folder_path)
    return Path(__file__).parents[2] / "data"


def get_default_log_filter() -> LogFilters:
    filter = LogFilters()
    filter.add_filter(
        where="cli_app",
        inside=[
            "add_files",
            "send_files",
            "send_file",
            "delete_files",
            "list_files",
            "download_files",
            "add_tags",
            "delete_tags",
        ],
    )
    return filter


app = App(name="A CLI app for interacting with the server file system")
def_filter = get_default_log_filter()
data_path: Path = get_data_path()
logger.remove()
logger.add(
    sys.stdout,
    filter=def_filter,
    colorize=True,
    backtrace=True,
)
# logger.add(
#     data_path / "logs.log",
#     filter=def_filter,
#     format=custom_log_format,
#     backtrace=True,
#     rotation="10 MB",
#     compression="zip",
#     serialize=True,
# )
my_logger = logger.bind(where="cli_app")
my_logger.info("Starting the CLI app")
watcher = ServiceWatcher()


async def get_server_address(
    watcher: ServiceWatcher,
    service_name: str,
) -> Optional[tuple[str, int]]:
    """Get the address of the server"""
    await asyncio.sleep(4)
    peers = watcher.get_services(service_name=service_name, max_amount=5)
    if len(peers) == 0:
        return None
    peer = rnd.choice(peers)
    ip, port = str(peer.service.ip), peer.service.port
    return ip, port


@app.command()
async def add_files(
    file_names: Annotated[
        list[str],
        Parameter(
            name="--files",
            consume_multiple=True,
        ),
    ],
    tags: Annotated[
        list[str],
        Parameter(
            name="--tags",
            consume_multiple=True,
        ),
    ],
):
    """Adds a list of files to the server with associated tags"""
    log = my_logger.bind(inside="add_files", files=file_names, tags=tags)
    print("Inside add files")
    log.info(f"Trying to upload the files ({file_names}) with tags ({tags})")

    res = await send_files(
        files_directory=Path(__file__).parents[2] / "files_to_send",
        file_names=file_names,
        tags=tags,
        my_logger=log,
        get_address=lambda: get_server_address(watcher, SERVER_SERVICE_NAME),
    )
    if res:
        log.info("Files uploaded successfully")
    else:
        log.error("An error occurred while uploading the files")


@app.command()
async def delete_files(
    tag_query: Annotated[
        list[str],
        Parameter(
            name="--tag-query",
            consume_multiple=True,
        ),
    ]
):
    """Deletes files from the server based on the tags"""
    pass


@app.command()
async def list_files(
    tag_query: Annotated[
        list[str],
        Parameter(
            name="--tag-query",
            consume_multiple=True,
        ),
    ]
):
    """Lists files from the server based on the tags"""
    pass


@app.command()
async def download_files(
    file_names: Annotated[
        list[str],
        Parameter(
            name="--files",
            consume_multiple=True,
        ),
    ]
):
    """Downloads files from the server based on the names of the files"""
    pass


@app.command()
async def add_tags(
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
    pass


@app.command()
async def delete_tags(
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
    pass


if __name__ == "__main__":
    app()
