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
from chord_subsystem.chord_interface import getShaRepr, in_between, ChordInterface
from async_sockets.async_connection import AsyncConnection
from server_constants import Commands
from file_operations import (
    handle_file_upload_to_server,
    handle_file_download_from_server,
    create_client,
    server_send_file,
)
from server_metadata_clases.file_server_metadata import FileServerFilesMeta
import json
import asyncio
import os
from socket import gethostbyaddr
import socket

PARTIAL_DOWN_FOLDER: str = "partial_downloads"
"""The name of the directory where all partial downloads gets stored"""
FILES_RECEIVED_FOLDER: str = "files_received"
"""The name of the directory where all the files received gets stored"""
METADATA_FOLDER: str = "metadata"
"""The name of the directory where all the metadata gets stored"""


def same_chord_nodes(nodes: list[ChordInterface], nodes2: list[ChordInterface]):
    """Tell if both lists have the same members"""
    return (
        len(nodes) == len(nodes2)
        and all(node in nodes2 for node in nodes)
        and all(node in nodes for node in nodes2)
    )


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

    async def start(self):
        """This method creates an infinite task for replicate data"""
        asyncio.create_task(self.successors_replication())

    async def replicate_files_with_successor(
        self,
        ip: str,
        port: int,
    ):
        """This method is the responsible for replicating the data to
        a successor with `ip:port`"""
        log = self.logger.bind(inside="replicate_files_with_successor")

        my_range: tuple[int, int] = await self.chord_node.get_range()

        my_files, _ = self.my_files.get_files_in_and_out_range(my_range)
        log.info(f"Sending the files to the successor")
        for file in my_files:
            await server_send_file(
                to_ip=ip,
                to_port=port,
                files_directory=self.storage_dir / FILES_RECEIVED_FOLDER,
                file_meta=file,
                my_logger=log,
                tags=[],
            )
        log.success("All the files have been sent to the successor")

    async def successors_replication(self):
        """This method is the responsible for replicating the data from
        to successors"""
        log = self.logger.bind(inside="successors_replication")
        task1: Optional[asyncio.Task] = None
        task2: Optional[asyncio.Task] = None
        successors: list[ChordInterface] = []
        while True:
            log.info("Starting the task of replicating to successors")
            if not self.chord_node.is_ready():
                log.warning("ChordNode is not ready yet. Waiting 3 seconds")
                await asyncio.sleep(3)
                continue
            log.success("ChordNode is ready. Checking the successors")
            new_successors = await self.chord_node.get_successors(
                length=3
            )  # It will always give 2 nodes (design choice)
            if not same_chord_nodes(successors, new_successors):
                log.info("The successors have changed")
                successors = new_successors
                if task1:
                    task1.cancel()
                if task2:
                    task2.cancel()
                task1 = asyncio.create_task(
                    self.replicate_files_with_successor(
                        ip=successors[0].ip,
                        port=successors[0].port,
                    )
                )
                task2 = asyncio.create_task(
                    self.replicate_files_with_successor(
                        ip=successors[1].ip,
                        port=successors[1].port,
                    )
                )
            elif not task1 or task1.done():
                log.info(
                    "The 1st successor have not changed. But we need to check if we can re-synchronize"
                )
                successor = successors[0]
                log.success(
                    f"Starting the synchronization with 1st successor at tcp://{successor.ip}:{successor.port} => {gethostbyaddr(successor.ip)}"
                )
                task1 = asyncio.create_task(
                    self.replicate_files_with_successor(
                        ip=successor.ip,
                        port=successor.port,
                    )
                )
            elif not task2 or task2.done():
                log.info(
                    "The 2nd successor have not changed. But But we need to check if we can re-synchronize"
                )
                successor2 = successors[0]
                log.success(
                    f"Starting the synchronization with 2nd successor at tcp://{successor2.ip}:{successor2.port} => {gethostbyaddr(successor.ip)}"
                )
                task2 = asyncio.create_task(
                    self.replicate_files_with_successor(
                        ip=successor2.ip,
                        port=successor2.port,
                    )
                )
            else:
                log.info("The successors have not changed. Waiting 3 seconds")
                await asyncio.sleep(3)

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

            if self.my_files.has_file(file_meta):
                log.warning("The file already exists in our database")
                await connection.send_multipart([b"ALREADY_EXISTS"])
                return

            log.success("Starting the upload of the file to our storage")

            await handle_file_upload_to_server(
                connection=connection,
                file_meta=file_meta,
                # tags=tags,
                partial_downloads_folder=self.storage_dir / PARTIAL_DOWN_FOLDER,
                received_files_folder=self.storage_dir / FILES_RECEIVED_FOLDER,
            )
            log.success("The file has been uploaded")
            self.my_files.add_files([file_meta])
            log.success("The file has been added to my database")
        elif from_peer_server:
            log.success(
                f"The key ({file_key}) doesn't belong to my range ({my_range}) but a peer server is sending it"
            )
            if self.my_files.has_file(file_meta):
                log.warning("The file already exists in our database")
                await connection.send_multipart([b"ALREADY_EXISTS"])
                return

            log.success("Starting the upload of the file to our storage")

            await handle_file_upload_to_server(
                connection=connection,
                file_meta=file_meta,
                # tags=tags,
                files_database=self.my_files,
                partial_downloads_folder=self.storage_dir / PARTIAL_DOWN_FOLDER,
                received_files_folder=self.storage_dir / FILES_RECEIVED_FOLDER,
            )
            log.success("The file has been uploaded as a replicated file")
            self.my_files.add_files([file_meta])
            log.success("The file has been added to my database")
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
                await self.handle_upload(
                    connection, file_metadata, tags, from_peer_server=from_peer_server
                )
            case Commands.DOWNLOAD.value, file_name_bytes:
                file_name: str = file_name_bytes.decode()
                await self.handle_download(connection, file_name)
            case _:
                log.warning(f"An unknown operation was received => {initial_request}")
