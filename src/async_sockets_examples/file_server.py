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

my_logger = logger.bind(where="file_server.py")
partial_download_dir: Path = Path(__file__).parent / "partial_downloads"
downloaded_files_dir: Path = Path(__file__).parent / "files_received"


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


async def download_file(connection: AsyncConnection, initial_message=list[bytes]):
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
        bytes_to_download = await file_download_helper.get_number_bytes_to_download()
        downloaded_bytes = 0
        while bytes_to_download - downloaded_bytes:
            to_download = min(
                500 * 1024, bytes_to_download - downloaded_bytes
            )  # MAX is 500 KB
            log.info(
                f"Asking the client to upload a chunk with offset {downloaded_bytes} and size {to_download} bytes"
            )
            await connection.send_multipart(
                [b"FETCH", downloaded_bytes.to_bytes(8), to_download.to_bytes(4)]
            )
            log.info("Fetch request sended")
            log.info("Waiting for the chunk...")
            response = await connection.recv_multipart()
            log.info(f"Client response received")
            if len(response) == 1 and response[0] == b"INVALID_CHUNK_REQUEST":
                raise Exception("Invalid chunk request by our side")
            assert (
                len(response) == 2
            ), "The request MUST be an `UPLOAD_CHUNK` with the chunk. But not the case"

            operation, chunk = response
            assert (
                operation == b"UPLOAD_CHUNK"
            ), f"The expected operation is 'UPLOAD_CHUNK' but it was received => '{operation}'"

            chunk_size = len(chunk)
            log.info(f"Received a chunk with size => {chunk_size}")
            log.debug(f"Writing the chunk in our storage")
            await file_download_helper.write_chunk(chunk=chunk)
            log.debug("Writing in storage was successful")

            downloaded_bytes += chunk_size
        log.info(
            f"All the bytes has been download => {downloaded_bytes}/{bytes_to_download}"
        )
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


async def start_server(ip: str, port: int):
    log = my_logger.bind(inside="start_server")
    log.info("Starting the server")
    server = AsyncServer(
        ip=ip,
        port=port,
        handle_client_connection=handle_client,
    )
    log.info("Server running")
    await server.run_server()


if __name__ == "__main__":
    ip: str = socket.gethostbyname(socket.gethostname())
    port = 8013
    asyncio.run(start_server(ip, port))
