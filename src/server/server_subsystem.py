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
from chord_subsystem.chord_interface import getShaRepr, in_between
from async_sockets.async_connection import AsyncConnection
from server_constants import Commands
from file_operations import (
    handle_file_upload_to_server,
    handle_file_download_from_server,
)
from server_metadata_clases.file_server_metadata import FileServerFilesMeta
import json
import asyncio
import os

PARTIAL_DOWN_FOLDER: str = "partial_downloads"
"""The name of the directory where all partial downloads gets stored"""
FILES_RECEIVED_FOLDER: str = "files_received"
"""The name of the directory where all the files received gets stored"""
METADATA_FOLDER: str = "metadata"
"""The name of the directory where all the metadata gets stored"""


class FileServerSubsystem:
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
        self.logger: Logger = logger.bind(
            where="FileServerSubsystem", ip=self.ip, port=self.port
        )
        """The logger for this service"""

        self.my_files: FileServerFilesMeta = FileServerFilesMeta()
        """The list of files this server has"""
        self.replica_files: FileServerFilesMeta = FileServerFilesMeta()
        """The list of files this server has as replicas"""

    async def am_responsible(self, file_name: str) -> tuple[bool, int, tuple[int, int]]:
        """
        Tells if this server is responsible for the file.

        returns a Tuple of [bool, int, tuple[int,int]] that represents
        if we are responsible for the file_name, what is the key associated
        with the file_name, and the current range of this node
        """
        key = getShaRepr(file_name)
        my_range: tuple[int, int] = await self.chord_node.get_range()
        return in_between(key, *my_range), key, my_range

    async def redirect_call(
        self,
        connection: AsyncConnection,
        file_key: int,
        my_range: tuple[int, int],
    ):
        """
        Redirects the call to the responsible server
        """
        log = self.logger.bind(inside="redirect_call")
        log.success(f"The key ({file_key}) doesn't belong to my range ({my_range})")
        log.info(f"Finding the successor of id ({file_key})")
        succ = await self.chord_node.find_successor(file_key)
        if not succ:
            log.warning("We couldn't obtain the successor")
            await connection.send_multipart([b"NOT_READY"])
            return
        log.success(f"Successor found => {succ}")
        await connection.send_multipart(
            [b"REDIRECTION", succ.ip.encode(), succ.port.to_bytes(4)]
        )
        log.success("A redirection message was sent")

    async def handle_upload(
        self,
        connection: AsyncConnection,
        file_meta: FileMetadata,
        tags: list[str],
        from_peer_server: bool = False,
    ) -> None:
        """
        Handles an upload request from a client

        :param file_meta: The metadata of the file to be uploaded
        :param tags: The tags of the file to be uploaded
        :param log: The logger for this method
        """
        log = self.logger.bind(inside="handle_upload")
        log.success(
            f"Handling an upload request for file ({file_meta.name}) with tags ({tags})"
        )
        # my_range: tuple[int, int] = await self.chord_node.get_range()
        # file_key: int = getShaRepr(file_meta.name)
        responsible, file_key, my_range = await self.am_responsible(
            file_name=file_meta.name
        )

        if responsible:
            log.success(f"The key ({file_key}) belong to my range {my_range}")
            log.success("Starting the upload of the file to our storage")
            await handle_file_upload_to_server(
                connection=connection,
                file_meta=file_meta,
                # tags=tags,
                partial_downloads_folder=self.storage_dir / PARTIAL_DOWN_FOLDER,
                received_files_folder=self.storage_dir / FILES_RECEIVED_FOLDER,
            )
            log.success("The file has been uploaded")
        elif from_peer_server:
            log.success(
                f"The key ({file_key}) doesn't belong to my range ({my_range}) but a peer server is sending it"
            )
            log.success("Starting the upload of the file to our storage")
            await handle_file_upload_to_server(
                connection=connection,
                file_meta=file_meta,
                # tags=tags,
                partial_downloads_folder=self.storage_dir / PARTIAL_DOWN_FOLDER,
                received_files_folder=self.storage_dir / FILES_RECEIVED_FOLDER,
            )
            log.success("The file has been uploaded as a replicated file")
        else:
            log.warning("Redirecting the request to another node")
            await self.redirect_call(
                connection=connection,
                file_key=file_key,
                my_range=my_range,
            )
        log.success("Operation completed")

    async def handle_download(
        self,
        connection: AsyncConnection,
        file_name: str,
    ):
        """
        Handles a download request from a client
        """
        log = self.logger.bind(inside="handle_download", file_name=file_name)
        log.success(f"Handling the download operation for the file ({file_name})")

        responsible, file_key, my_range = await self.am_responsible(
            file_name=file_name,
        )

        if responsible:
            log.success(f"The key ({file_key}) belong to my range {my_range}")
            await handle_file_download_from_server(
                connection=connection,
                file_name=file_name,
                received_files=self.storage_dir / FILES_RECEIVED_FOLDER,
            )
        else:
            log.warning(f"Redirecting the request to another node")
            await self.redirect_call(
                connection=connection,
                file_key=file_key,
                my_range=my_range,
            )
        log.success("Operation completed")

    async def handle_request(
        self,
        connection: AsyncConnection,
        initial_request: list[bytes],
        from_peer_server: bool = False,
    ) -> Optional[list[bytes]]:
        """
        Handles a request from a client

        :param request: The request from the client
        :param from_peer_server: Tells if the request is made by another peer server
        :return: The response to the client
        """
        log = self.logger.bind(inside="handle_request")
        log.debug(f"Handling request: {initial_request}")

        assert len(initial_request) > 0, "Request is empty"

        match initial_request:
            case Commands.UPLOAD.value, file_meta_bytes, tags_bytes:
                log.info("Handling an upload request")
                file_metadata = FileMetadata.from_json(file_meta_bytes)
                tags = json.loads(tags_bytes)
                await self.handle_upload(connection, file_metadata, tags)
            case Commands.DOWNLOAD.value, file_name_bytes:
                file_name: str = file_name_bytes.decode()
                await self.handle_download(connection, file_name)
            case _:
                log.warning(f"An unknown operation was received => {initial_request}")
