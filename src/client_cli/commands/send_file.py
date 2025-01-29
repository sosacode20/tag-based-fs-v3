from pathlib import Path
from loguru._logger import Logger
from async_sockets.async_connection import AsyncConnection
from .create_client import create_client
from file_helpers.file_helper import FileHelper, FileMetadata
import asyncio
import json


async def send_files(
    to_ip: str,
    to_port: int,
    files_directory: Path,
    file_names: list[str],
    tags: list[str],
    my_logger: Logger,
) -> bool:
    log = my_logger.bind(inside="send_files", files=file_names, tags=tags)
    successful = False
    try:
        log.info(f"Connecting to => {to_ip}:{to_port}")
        client = await create_client(to_ip=to_ip, to_port=to_port, my_logger=log)
        log.info(f"Sending files to => {to_ip}:{to_port}")
        for file_name in file_names:
            await send_file(
                to_ip=to_ip,
                to_port=to_port,
                files_directory=files_directory,
                file_name=file_name,
                tags=tags,
                my_logger=log,
            )
        log.info("All files has been sended")
        successful = True
    except asyncio.TimeoutError:
        log.error("The connection with server reached the timeout")
    except asyncio.CancelledError:
        log.error("User pressed CTRL+C and CLI is stopping")
        client.close_connection()
        log.info("Connection closed")
    except Exception as e:
        log.error(f"An unknown exception occur =>\n{repr(e)}")
    finally:
        log.info("Closing connection with server")
        client.close_connection()
        log.info("Connection closed")
        return successful


async def send_file(
    client: AsyncConnection,
    files_directory: Path,
    file_name: str,
    tags: list[str],
    my_logger: Logger,
    timeout=500,
):
    log = my_logger.bind(inside="send_file", file_name=file_name, tags=tags)
    log.info(f"Trying to send a file with name {file_name} to the server")
    file_path = files_directory / file_name
    if not file_path.exists():
        # print(f"The file at path {str(file_path)} doesn't exist")
        log.warning(f"The file at path {str(file_path)} doesn't exist")
        return
    # client: AsyncConnection = await create_client(
    #     to_ip=to_ip, to_port=to_port, my_logger=my_logger
    # )
    file_helper: FileHelper = FileHelper(file_path=file_path)
    file_metadata: FileMetadata = file_helper.get_metadata()
    log.info(f"Sending the headers of the file to upload")
    await client.send_multipart(
        message=[
            b"UPLOAD",
            file_metadata.to_json().encode(),
            json.dumps(tags).encode(),
        ]
    )
    log.info("Waiting for server to accept the file")
    response = await client.recv_multipart()
    allowed, _ = response
    if allowed != b"OK":
        log.info("The server don't allow the file upload")
        return
    log.info("The server wants the file")
    log.info("Sending the file")
    await client.send_file(file_helper=file_helper, timeout=timeout)
    log.info("File sended")
