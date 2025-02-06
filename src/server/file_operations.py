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
from server.server_metadata_clases.file_server_metadata import FileServerFilesMeta
import socket
from socket import gethostbyaddr

my_logger = logger.bind(where="file_operations")


async def create_client(to_ip: str, to_port: int, my_logger: Logger) -> AsyncConnection:
    """Create an AsyncConnection that serves as a client connecting to a server"""
    log = my_logger.bind(inside="create_client", to_ip=to_ip, to_port=to_port)
    log.info("Creating the client")
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setblocking(False)
    address = (to_ip, to_port)
    log.info("Connecting to server")
    # client_socket.connect(address)
    loop = asyncio.get_running_loop()
    await loop.sock_connect(client_socket, address=address)
    log.info("Connection Successful")
    return AsyncConnection(connection_socket=client_socket, address=address)


# async def proxy(start_connection: AsyncConnection, end_connection)


async def handle_file_upload_to_server(
    connection: AsyncConnection,
    file_meta: FileMetadata,
    # tags: list[str],
    # files_database: FileServerFilesMeta,
    partial_downloads_folder: Path,
    received_files_folder: Path,
):
    """This function is for uploading a file to the server.

    The server uses the `recv_file` functionality of the `AsyncSockets`"""
    log = my_logger.bind(inside="handle_file_upload_to_server")
    log.info(f"Handling an upload request for file ({file_meta.name})")

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
    log = my_logger.bind(inside="handle_file_download_from_server")
    log.info(f"Handling a download request for file ({file_name})")
    file_path = received_files / file_name
    if not file_path.exists():
        log.warning(f"The file ({file_name}) doesn't exist")
        await connection.send_multipart([b"NOT_FOUND"])
        return
    file_helper: FileHelper = FileHelper(file_path=file_path)
    file_meta: FileMetadata = file_helper.get_metadata()
    log.info(f"Sending the metadata of the file to download")
    await connection.send_multipart([b"OK", file_meta.to_json().encode()])
    log.info("Metadata sent")

    log.info("Ready for sending the file")
    res = await connection.send_file(
        file_helper=file_helper,
        timeout=20,
    )
    if not res:
        log.warning(f"The file ({file_name}) was not sent successfully")
        return False
    log.success(f"The file ({file_name}) has been sent successfully")
    return True


async def server_send_file(
    # client: AsyncConnection,
    to_ip: str,
    to_port: int,
    files_directory: Path,
    # file_name: str,
    file_meta: FileMetadata,
    my_logger: Logger,
    tags: list[str] = [],
    timeout=20,
):
    log = my_logger.bind(inside="server_send_file", file_name=file_meta.name, tags=tags)
    log.info(
        f"Trying to send a file with name ({file_meta.name}) and tags({tags}) to the server"
    )
    file_path = files_directory / file_meta.name
    if not file_path.exists():
        # print(f"The file at path {str(file_path)} doesn't exist")
        log.warning(f"The file at path {str(file_path)} doesn't exist")
        return False
    try:
        log.info("Creating the client")
        client = await create_client(to_ip, to_port, my_logger=my_logger)
        log.success(
            f"Client created at tcp://{to_ip}:{to_port} => {gethostbyaddr(to_ip)}"
        )
    except Exception as e:
        log.error(f"An error occur while creating the client => {repr(e)}")
        return False
    file_helper: FileHelper = FileHelper(file_path=file_path)
    # file_metadata: FileMetadata = file_helper.get_metadata()
    log.info(f"Sending the headers of the file to upload")
    await asyncio.wait_for(
        client.send_multipart(
            message=[
                b"FILES",
                b"FILES",
                Commands.UPLOAD.value,
                file_meta.to_json().encode(),
                json.dumps(tags).encode(),
            ]
        ),
        timeout=timeout,
    )
    log.info("Waiting for server to accept the file")
    response = await asyncio.wait_for(
        client.recv_multipart(),
        timeout=timeout,
    )
    log.debug(f"Received response from server => {response}")

    match response:
        case b"REDIRECTION", ip, port:
            ip = ip.decode()
            port = int.from_bytes(port)
            log.info(f"Client will be redirected to {ip}:{port} => {gethostbyaddr(ip)}")
            log.warning("This redirection SHOULD NOT be happening")
            return await server_send_file(
                # client=client,
                to_ip=ip,
                to_port=port,
                files_directory=files_directory,
                # file_name=file_name,
                file_meta=file_meta,
                tags=tags,
                my_logger=my_logger,
                timeout=timeout,
            )
        case b"ALREADY_EXISTS",:
            log.success(
                "The server has this exact same version of the file. So no need to uploaded again"
            )
            return True
        case b"OK",:
            log.info("The server wants the file")
            log.info("Sending the file")
            await client.send_file(file_helper=file_helper, timeout=timeout)
            log.info("File sended")
            return True
        case b"NOT_READY",:
            log.warning(
                "The server is not ready to accept the file. Wait for some time and try again"
            )
            return False
        case b"NOT_ALLOWED":
            log.warning("The server doesn't want the file")
            return False
        case _:
            log.warning(f"The server don't allow the file upload => ({response})")
            return False


# async def get_file_tags(
#     connection: AsyncConnection,
#     file_name: str,
#     received_files: Path,
# ):
#     pass
