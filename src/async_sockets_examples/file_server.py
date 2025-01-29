from basic_imports import *
import asyncio
import socket
from async_sockets.async_server import AsyncServer, AsyncConnection
from loguru import logger
from file_helpers.file_helper import FileMetadata, FileHelper
from file_helpers.file_download_helper import FileDownloadHelper, DownloadMetadata
from pydantic import ValidationError
from pathlib import Path
from service_discovery.service_watcher import ServiceWatcher
from cyclopts import App, Parameter
import os
from typing import Optional


def get_data_base_folder() -> Path:
    """Returns the Path where the data should be stored"""
    folder_path = os.getenv("DATA_FOLDER")
    if folder_path:
        return Path(folder_path)
    return Path(__file__).parents[2] / "data"


app = App("A CLI app for creating a file server")
my_logger = logger.bind(where="file_server.py")

data_base_folder: Path = get_data_base_folder()
partial_download_dir: Path = data_base_folder / "partial_downloads"
downloaded_files_dir: Path = data_base_folder / "files_received"


async def download_file2(connection: AsyncConnection, initial_message=list[bytes]):
    log = my_logger.bind(inside="download_file")
    operation, metadata_bytes = initial_message
    assert (
        operation == b"UPLOAD"
    ), "The initial operation for downloading a file in the server, MUST be 'UPLOAD'"
    try:
        metadata: FileMetadata = FileMetadata.from_json(metadata_bytes)
        file_download_helper = FileDownloadHelper(
            file_name=metadata.name,
            storage_directory=partial_download_dir,
            download_metadata=DownloadMetadata.from_file_metadata(metadata),
        )
        await file_download_helper.initialize()
        log.info("Sending a confirmation for the UPLOAD of file")
        await connection.send_multipart([b"OK", metadata_bytes])
        log.info("Confirmation sended")
        log.info("Download starts now...")

        await connection.recv_file(
            file_download_helper=file_download_helper,
            max_chunk_size=1 * 1024**2,
            timeout=500,
        )

        log.info(f"The file has been downloaded")
        log.info("Sending a message of finish operation to the client")
        await connection.send_multipart([b"COMPLETED"])
        log.info("Message delivered")

        log.info(f"Checking integrity of file")
        if await file_download_helper.is_download_correct():
            log.info("The download of the file was successful")
            await file_download_helper.save_completed_file(
                destination_folder=downloaded_files_dir
            )
            log.info("File saved successfully")
        else:
            log.info("The downloaded file is corrupted")
            await file_download_helper.save_completed_file(
                destination_folder=downloaded_files_dir,
            )

    except ValidationError as e:
        log.error(f"A validation error occurred in the parsing of the metadata")
        log.info("Closing connection")
        connection.close_connection()
        log.info("Connection closed")
    except asyncio.CancelledError:
        log.info("User cancelled the task. Closing connection")
        connection.close_connection()
        log.info("Connection closed")
        raise
    except Exception as e:
        log.error(f"An unknown error has occurred => {str(e)}")
        connection.close_connection()
        log.info("Connection Closed")
        raise
        # TODO: Check if the exception should be raised again


async def handle_client(connection: AsyncConnection):
    log = my_logger.bind(inside="handle_client", connection=connection)
    log.info("Waiting for message")
    try:
        # empty = await asyncio.wait_for(connection.recv_multipart(), timeout=5)
        # log.info(f"Received the 'empty message' => {empty}")
        client_message = await asyncio.wait_for(connection.recv_multipart(), timeout=20)
        assert len(client_message) == 2, "The client message MUST have 2 parts"
        operation, _ = client_message
        log.info(f"Message Received => {client_message}")
        if operation == b"UPLOAD":
            # await download_file(connection=connection, initial_message=client_message)
            await download_file2(connection=connection, initial_message=client_message)
        else:
            log.warning(f"Operation '{operation}' is not implemented")

    except asyncio.TimeoutError:
        log.error(f"Timeout reached in connection {connection}")
    except KeyboardInterrupt:
        raise
    except Exception as e:
        log.error(f"An unknown error has occurred => {e}")


@app.command()
async def start_server(port: int = 8013):
    log = my_logger.bind(inside="start_server")
    log.info("Starting the server")
    ip: str = socket.gethostbyname(socket.gethostname())

    server = AsyncServer(
        ip=ip,
        port=port,
        handle_client_connection=handle_client,
    )
    log.info("Server running")
    await server.run_server()


if __name__ == "__main__":
    app()
