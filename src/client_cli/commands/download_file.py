from pathlib import Path
from loguru._logger import Logger
from async_sockets.async_connection import AsyncConnection
from .create_client import create_client
from file_helpers.file_helper import FileHelper, FileMetadata
from file_helpers.file_download_helper import FileDownloadHelper, DownloadMetadata
import asyncio
import json
from .constants import Commands, CLIENT_SYSTEM
from typing import Callable, Coroutine, Any, Optional
from socket import gethostbyaddr


async def download_files(
    file_names: list[str],
    my_logger: Logger,
    partial_download_folder: Path,
    received_files_folder: Path,
    get_address: Callable[[], Coroutine[Any, Any, Optional[tuple[str, int]]]],
    timeout: float = 20,
) -> bool:
    """
    Download files from the server.
    """
    log = my_logger.bind(inside="download_files")
    log.success(f"Starting the `download_files` operation")

    for file_name in file_names:
        log.info("Getting a file server address")
        address = await get_address()
        if not address:
            log.error("No server found. Exiting app")
            return False
        to_ip, to_port = address
        log.info(f"Server found at {to_ip}:{to_port} => {gethostbyaddr(to_ip)}")
        log.info("Starting the operation")

        try:
            await download_file(
                to_ip=to_ip,
                to_port=to_port,
                file_name=file_name,
                partial_downloads_folder=partial_download_folder,
                received_files_folder=received_files_folder,
                my_logger=log,
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.error("The connection with server reached the timeout")
        except asyncio.CancelledError:
            log.error("User pressed CTRL+C and CLI is stopping")
        except Exception as e:
            log.error(f"An unknown exception occur =>\n{repr(e)}")
    
    return True


async def download_file(
    to_ip: str,
    to_port: int,
    file_name: str,
    partial_downloads_folder: Path,
    received_files_folder: Path,
    my_logger: Logger,
    timeout: float = 20,
):
    """This function is for uploading a file to the server.

    The server uses the `recv_file` functionality of the `AsyncSockets`
    """
    log = my_logger.bind(inside="download_file")
    log.info(f"Trying to download file ({file_name})")
    try:
        log.info(f"Connecting to => tcp://{to_ip}:{to_port}")
        connection = await create_client(to_ip, to_port, my_logger)
        log.success(
            f"Client created at tcp://{to_ip}:{to_port} => {gethostbyaddr(to_ip)}"
        )
    except Exception as e:
        log.error(f"Error while connecting to => tcp://{to_ip}:{to_port}")
        return False

    log.info("Sending the download request")
    await asyncio.wait_for(
        connection.send_multipart(
            [
                CLIENT_SYSTEM,
                b"FILES",
                Commands.DOWNLOAD.value,
                file_name.encode(),
            ]
        ),
        timeout=timeout,
    )
    log.info("Request sent")

    log.info("Waiting for the confirmation")
    response = await asyncio.wait_for(
        connection.recv_multipart(),
        timeout=timeout,
    )

    match response:
        case b"REDIRECTION", ip, port:
            ip = ip.decode()
            port = int.from_bytes(port)
            log.info(f"Client will be redirected to {ip}:{port} => {gethostbyaddr(ip)}")
            return await download_file(
                to_ip=ip,
                to_port=port,
                file_name=file_name,
                partial_downloads_folder=partial_downloads_folder,
                received_files_folder=received_files_folder,
                my_logger=my_logger,
                timeout=timeout,
            )
        case b"NOT_READY":
            log.error("The server is not ready to send the file")
            return False
        case b"NOT_FOUND":
            log.error("The file was not found on the server")
            return False
        case b"OK", file_metadata_bytes:
            log.success("The server is ready to send the file")
            file_metadata = FileMetadata.from_json(file_metadata_bytes)
            # log.debug(
            #     f"The partial downloads folder is => {str(partial_downloads_folder)}"
            # )
            file_download_helper = FileDownloadHelper(
                file_name=file_metadata.name,
                storage_directory=partial_downloads_folder,
                download_metadata=DownloadMetadata.from_file_metadata(file_metadata),
            )
            await file_download_helper.initialize()
            log.success(f"Starting the download of the file ({file_name})")
            res = await connection.recv_file(
                file_download_helper=file_download_helper,
                max_chunk_size=2 * 1024**2,
                timeout=timeout,
            )
            if not res:
                log.warning(f"The file ({file_name}) was not downloaded completely")
                return False
            log.success(f"The file ({file_name}) has been downloaded")

            log.info(f"Checking for the integrity of the file")
            if await file_download_helper.is_download_correct():
                log.info("The download of the file was successful")
                await file_download_helper.save_completed_file(
                    destination_folder=received_files_folder,
                )
                log.info("file saved successfully")
            else:
                log.warning("The downloaded file is corrupted")
            return True
