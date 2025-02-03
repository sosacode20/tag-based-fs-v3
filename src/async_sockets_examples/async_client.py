from basic_imports import *
import socket
import asyncio
from pathlib import Path
from async_sockets.async_connection import AsyncConnection
from cyclopts import App, Parameter
from rich import print
from loguru import logger
from file_helpers.file_helper import FileHelper, FileMetadata

# from file_helpers.file_download_helper import DownloadMetadata, FileDownloadHelper
from service_discovery.service_watcher import ServiceWatcher
import os
from typing import Optional


def get_data_base_folder() -> Path:
    """Returns the Path where the data should be stored"""
    folder_path = os.getenv("DATA_FOLDER")
    if folder_path:
        return Path(folder_path)
    return Path(__file__).parents[2] / "data"


app = App(name="Simple CLI for sending files")
data_base_folder = get_data_base_folder()
files_directory = data_base_folder / "files_to_send"
my_logger = logger.bind(where="async_client.py")

discovery = ServiceWatcher()


async def create_client(to_ip: str, to_port: int) -> AsyncConnection:
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


@app.command()
async def send_file(to_ip: str, to_port: int, file_name: str, timeout=500):
    log = my_logger.bind(inside="send_file")
    log.info("Trying to send a file")
    file_path = files_directory / file_name
    if not file_path.exists():
        # print(f"The file at path {str(file_path)} doesn't exist")
        log.info(f"The file at path {str(file_path)} doesn't exist")
        return
    try:
        client: AsyncConnection = await create_client(to_ip=to_ip, to_port=to_port)
        file_helper: FileHelper = FileHelper(file_path=file_path)
        file_metadata: FileMetadata = file_helper.get_metadata()

        # await client.send_multipart([b""])

        log.info(f"Sending the headers of the file to upload")
        await client.send_multipart(
            message=[b"UPLOAD", file_metadata.to_json().encode()]
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

    except asyncio.TimeoutError:
        log.error("The connection with server reached the timeout")
    except asyncio.CancelledError:
        log.error("User pressed CTRL+C and CLI is stopping")
        client.close_connection()
        log.info("Connection closed")
    except Exception as e:
        log.error(f"An unknown exception occur =>\n{repr(e)}")
        log.error(f"{e.with_traceback()}")
    finally:
        log.info("Closing connection with server")
        client.close_connection()
        log.info("Connection closed")


if __name__ == "__main__":
    app()
