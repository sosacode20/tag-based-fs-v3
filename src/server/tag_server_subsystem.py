from basic_imports import *
from typing import Optional
from logging_helper.logging_utils import LogFilters, custom_log_format
from cyclopts import App, Parameter
from loguru import logger
from loguru._logger import Logger
from pathlib import Path
from file_helpers.file_download_helper import DownloadMetadata, FileDownloadHelper
from file_helpers.file_helper import FileMetadata, FileHelper
from chord_subsystem.chord_node import ChordNode
from async_sockets.async_connection import AsyncConnection
from server_constants import Commands
from pathlib import Path
import asyncio
import os

PARTIAL_DOWN_FOLDER: str = "partial_downloads"
"""The name of the directory where all partial downloads gets stored"""
FILES_RECEIVED_FOLDER: str = "files_received"
"""The name of the directory where all the files received gets stored"""
METADATA_FOLDER: str = "metadata"
"""The name of the directory where all the metadata gets stored"""


class TagServerSubsystem:
    def __init__(
        self,
        ip: str,
        port: int,
        chord_node: ChordNode,
        storage_directory: Path,
    ):
        self.ip: str = ip
        """The IP of this service"""
        self.port: int = port
        """The port of this service"""
        self.chord_node: ChordNode = chord_node
        """The ChordNode object that this service is using"""
        self.storage_dir: Path = storage_directory
        """The base folder for storing all the file server data"""

    async def handle_request(
        self,
        connection: AsyncConnection,
        initial_request: list[bytes],
    ) -> None:
        """
        Handles a request from a client

        :param connection: The connection object for the client
        :param initial_message: The initial message from the client
        """
        log = logger.bind(where="TagServerSubsystem", ip=self.ip, port=self.port)
        operation, *data = initial_request
        if operation == Commands.ADD_TAGS:
            await self.add_tags(connection, data, log)
        elif operation == Commands.DELETE_TAGS:
            await self.delete_tags(connection, data, log)
        else:
            log.error(f"Unknown operation: {operation}")
