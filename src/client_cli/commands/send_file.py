from pathlib import Path
from loguru._logger import Logger
from typing import Callable, Coroutine, Any, Optional
from async_sockets.async_connection import AsyncConnection
from .create_client import create_client
from file_helpers.file_helper import FileHelper, FileMetadata
import asyncio
import json
from .constants import Commands, CLIENT_SYSTEM
from socket import gethostbyaddr


async def send_files(
    files_directory: Path,
    file_names: list[str],
    tags: list[str],
    my_logger: Logger,
    get_address: Callable[[], Coroutine[Any, Any, Optional[tuple[str, int]]]],
) -> bool:
    """
    Send files to the server.

    :param files_directory: The directory where the files are stored
    :param file_names: The names of the files to be sent
    :param tags: The tags of the files
    :param my_logger: The logger for this function
    :return: True if the files were sent successfully, False otherwise
    """
    log = my_logger.bind(inside="send_files", files=file_names, tags=tags)
    successful = False
    log.info(f"Starting the `sending files` operation")

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
            await send_file(
                # client=client,
                to_ip=to_ip,
                to_port=to_port,
                files_directory=files_directory,
                file_name=file_name,
                tags=tags,
                my_logger=log,
            )
        except asyncio.TimeoutError:
            log.error("The connection with server reached the timeout")
        except asyncio.CancelledError:
            log.error("User pressed CTRL+C and CLI is stopping")
        except Exception as e:
            log.error(f"An unknown exception occur =>\n{repr(e)}")

    log.info("All files has been sended")
    return True


async def send_file(
    # client: AsyncConnection,
    to_ip: str,
    to_port: int,
    files_directory: Path,
    file_name: str,
    tags: list[str],
    my_logger: Logger,
    timeout=20,
):
    log = my_logger.bind(inside="send_file", file_name=file_name, tags=tags)
    log.info(
        f"Trying to send a file with name ({file_name}) and tags({tags}) to the server"
    )
    file_path = files_directory / file_name
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
    file_metadata: FileMetadata = file_helper.get_metadata()
    log.info(f"Sending the headers of the file to upload")
    await asyncio.wait_for(
        client.send_multipart(
            message=[
                CLIENT_SYSTEM,
                b"FILES",
                Commands.UPLOAD.value,
                file_metadata.to_json().encode(),
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
            return await send_file(
                # client=client,
                to_ip=ip,
                to_port=port,
                files_directory=files_directory,
                file_name=file_name,
                tags=tags,
                my_logger=my_logger,
                timeout=timeout,
            )
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
            log.warning("The server don't allow the file upload")
            return False
