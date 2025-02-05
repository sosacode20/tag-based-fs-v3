from basic_imports import *
from typing import Optional
from pathlib import Path
from loguru._logger import Logger
from loguru import logger
from async_sockets.async_connection import AsyncConnection
from file_helpers.file_download_helper import DownloadMetadata, FileDownloadHelper
from file_helpers.file_helper import FileMetadata, FileHelper
from server_constants import Commands
import json
import asyncio

my_logger = logger.bind(where="file_operations")


async def handle_file_upload_to_server(
    connection: AsyncConnection,
    file_meta: FileMetadata,
    tags: list[str],
    partial_downloads_folder: Path,
    received_files_folder: Path,
):
    """This function is for uploading a file to the server.

    The server uses the `recv_file` functionality of the `AsyncSockets`"""
    log = my_logger.bind(inside="handle_file_upload_to_server")
    log.info(
        f"Handling an upload request for file ({file_meta.name}) with tags ({tags})"
    )
    file_download_helper: FileDownloadHelper = FileDownloadHelper(
        file_name=file_meta.name,
        storage_directory=partial_downloads_folder,
        download_metadata=DownloadMetadata.from_file_metadata(file_meta),
    )
    await file_download_helper.initialize()
    log.info("Sending a confirmation for the UPLOAD of file")
    await connection.send_multipart([b"OK"])
    log.info("Confirmation sended")
    log.info("Download Starts now...")
    completed, _ = await connection.recv_file(
        file_download_helper=file_download_helper,
        max_chunk_size=2 * 1024**2,
        timeout=20,
    )
    if not completed:
        log.warning(f"The file ({file_meta.name}) was not downloaded completed")
        return
    log.info(f"The file has been downloaded")

    log.info(f"Checking for the integrity of the file")
    if await file_download_helper.is_download_correct():
        log.info("The download of the file was successful")
        await file_download_helper.save_completed_file(
            destination_folder=received_files_folder,
        )
        log.info("file saved successfully")
    else:
        log.warning("The downloaded file is corrupted")


async def handle_file_download_from_server(
    connection: AsyncConnection,
    file_name: str,
    received_files: Path,
):
    """
    This function is for downloading a file from the server.
    """


async def send_file_to_peer_server(
    ip: str,
    port: int,
    file_name: str,
    tags: list[str],
):
    """Send a file with tags to a peer server.

    The peer server will be considered a replica"""
    pass


# async def get_file_tags(
#     connection: AsyncConnection,
#     file_name: str,
#     received_files: Path,
# ):
#     pass
