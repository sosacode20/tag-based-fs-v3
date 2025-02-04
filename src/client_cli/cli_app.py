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
from service_discovery.service_watcher import ServiceWatcher


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
logger.remove()
logger.add(
    sys.stdout,
    filter=def_filter,
    colorize=True,
    backtrace=True,
)
logger.add(
    "logs.log",
    filter=def_filter,
    format=custom_log_format,
    backtrace=True,
    rotation="10 MB",
    compression="zip",
    serialize=True,
)

data_path: Path = get_data_path()
watcher = ServiceWatcher()


@app.command()
async def add_files(
    file_names: Annotated[
        list[str],
        Parameter(
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
    """Adds a list of files to the server with associated tags"""
    pass


async def delete_files(
    tag_query: Annotated[
        list[str],
        Parameter(
            consume_multiple=True,
        ),
    ]
):
    """Deletes files from the server based on the tags"""
    pass


async def list_files(
    tag_query: Annotated[
        list[str],
        Parameter(
            consume_multiple=True,
        ),
    ]
):
    """Lists files from the server based on the tags"""
    pass


async def download_files(
    file_names: Annotated[
        list[str],
        Parameter(
            consume_multiple=True,
        ),
    ]
):
    """Downloads files from the server based on the names of the files"""
    pass


async def add_tags(
    tag_query: Annotated[
        list[str],
        Parameter(
            consume_multiple=True,
        ),
    ],
    tag_list: Annotated[
        list[str],
        Parameter(
            consume_multiple=True,
        ),
    ],
):
    """Adds tags to files based on the tag query"""
    pass


async def delete_tags(
    tag_query: Annotated[
        list[str],
        Parameter(
            consume_multiple=True,
        ),
    ],
    tag_list: Annotated[
        list[str],
        Parameter(
            consume_multiple=True,
        ),
    ],
):
    """Deletes tags from files based on the tag query"""
    pass
