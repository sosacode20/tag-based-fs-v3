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
import asyncio
import os


class FileServerSubsystem:
    def __init__(
        self,
        ip: str,
        port: int,
        chord_node: ChordNode,
    ):
        self.ip: str = ip
        """The IP of this service"""
        self.port: int = port
        """The port of this service"""
        self.chord_node: ChordNode = chord_node
        """The ChordNode object that this service is using"""
        self.logger: Logger = logger.bind(
            where="FileServerSubsystem", ip=self.ip, port=self.port
        )
        """The logger for this service"""

    async def handle_request(self, request: list[bytes]) -> Optional[list[bytes]]:
        """
        Handles a request from a client

        :param request: The request from the client
        :return: The response to the client
        """
        log = self.logger.bind(inside="handle_request")
        log.debug(f"Handling request: {request}")

        assert len(request) > 0, "Request is empty"

        # match request:
        #     case 
