from basic_imports import *
from logging_helper.logging_utils import LogFilters, custom_log_format, configure_logger
from cyclopts import App, Parameter
from loguru import logger
from loguru._logger import Logger
from pathlib import Path
from file_helpers.file_download_helper import DownloadMetadata, FileDownloadHelper
from file_helpers.file_helper import FileMetadata, FileHelper
import os
from service_discovery.service_watcher import ServiceWatcher, ServiceAnnouncement
from socket import gethostbyname, gethostname
from server_constants import SERVER_SERVICE_NAME, Subsystems, NOT_READY
from async_sockets.async_server import AsyncServer, AsyncConnection
from chord_subsystem.chord_node import ChordNode, CHORD_SERVICE_NAME, CHORD_SUBSYSTEM
from server_subsystem import FileServerSubsystem
from tag_server_subsystem import TagServerSubsystem


import asyncio
import zmq
from zmq.asyncio import Poller
from typing import Optional, Any

FROM_CLIENT = b"CLIENT"
"""The tag to indicate that the message is from a client"""


def get_data_base_folder() -> Path:
    """Returns the Path where the data should be stored"""
    folder_path = os.getenv("DATA_FOLDER")
    if folder_path:
        return Path(folder_path)
    return Path(__file__).parents[2] / "data"


def get_default_filter() -> LogFilters:
    filter = LogFilters()
    filter.add_filter(
        where="ChordNode",
        inside=[
            # "where_to_join",
            # "join",
            "stabilize",
            # "notify",
            # "fix_fingers",
            # "check_predecessor",
            # "find_successor",
            # "find_predecessor",
            # "closest_preceding_finger",
            # "handle_request",
        ],
    )
    # filter.add_filter(
    #     where="server_service.py",
    #     inside=[
    #         "handle_client",
    #         "start_server",
    #     ],
    # )
    # filter.add_filter(
    #     where="SuccessorList",
    #     inside=[
    #         "add",
    #         "get_successor",
    #         "get_pred_and_successors",
    #         "get_first_alive_successor",
    #         "update",
    #     ],
    # )
    filter.add_filter(
        where="FileServerSubsystem",
        inside=[
            "handle_request",
            "handle_upload",
            "successors_replication",
            "replicate_files_with_successor",
        ],
    )
    filter.add_filter(
        where="file_operations",
        inside=[
            "handle_file_upload_to_server",
        ],
    )
    return filter


def get_service_watcher(our_port: int) -> ServiceWatcher:
    return ServiceWatcher(
        services_to_announce=[
            ServiceAnnouncement(
                service=SERVER_SERVICE_NAME,
                ip=gethostbyname(gethostname()),
                port=our_port,
            )
        ]
    )


default_filter = get_default_filter()
data_path = get_data_base_folder()
app = App("A CLI app for creating a file server")
logger.remove()
logger.add(
    sys.stdout,
    filter=default_filter,
    # format=custom_log_format,
    colorize=True,
    backtrace=True,
    catch=True,
    diagnose=True,
)
# configure_logger()
logger.add(
    data_path / "logs" / "server_only.log",
    filter=default_filter,
    format=custom_log_format,
    backtrace=True,
    rotation="50 MB",
    # compression="zip",
    serialize=True,
)
my_logger = logger.bind(where="server_service.py")
zmq_context = zmq.SyncContext.instance()


async def handle_client(
    connection: AsyncConnection,
    chord_subsystem: ChordNode,
    file_server: FileServerSubsystem,
    tag_server: TagServerSubsystem,
):
    log = my_logger.bind(inside="handle_client")
    log.info(f"Handling the client with address => {connection.address}")
    try:
        request = await connection.recv_multipart()
        # NOTE: All request MUST be of the form [FROM, TO, *REST_REQUEST]
        match request:
            case b"PING",:
                log.info("The request is a PING")
                await connection.send_multipart([b"PONG"])
            case _, Subsystems.CHORD.value, *rest:
                log.info("The request is for the Chord subsystem")
                response = await chord_subsystem.handle_request(rest)
                if response:
                    await connection.send_multipart(response)
            case FROM, Subsystems.FILES.value, *rest:
                log.info("The request is for the Files subsystem")
                if not chord_subsystem.is_ready():
                    log.error(
                        "The Chord subsystem is not ready. So the request cannot proceed"
                    )
                    await connection.send_multipart([NOT_READY])
                    return
                await file_server.handle_request(
                    connection=connection,
                    initial_request=rest,
                    from_peer_server=True if FROM == Subsystems.FILES.value else False,
                )
            case _, Subsystems.TAGS.value, *rest:
                log.info("The request is for the Tags subsystem")
                if not chord_subsystem.is_ready():
                    log.error(
                        "The Chord subsystem is not ready. So the request cannot proceed"
                    )
                    await connection.send_multipart([NOT_READY])
                    return
                await tag_server.handle_request(
                    connection=connection,
                    initial_request=rest,
                )
            case _:
                log.warning("The request is not for any subsystem")
                log.warning(f"The request is => {request}")
        return
    # except BaseException as e:
    #     log.exception(f"Error while handling the client:\n{e.with_traceback()}")
    except Exception as e:
        log.error(f"An error occurred while handling the client =>\n{repr(e)}")
    finally:
        if connection:
            connection.close_connection()


@app.command()
async def start_server(port: int = 5700):
    """This function starts the server service"""
    log = my_logger.bind(inside="start_server")
    log.info("Starting the service watcher")
    watcher = get_service_watcher(our_port=port)
    ip = gethostbyname(gethostname())

    log.info("Creating the Chord Node")
    chord_sub = ChordNode(
        ip=ip,
        port=port,
        watcher=watcher,
    )
    asyncio.create_task(chord_sub.run())

    log.info("Creating the File Server subsystem")
    file_server = FileServerSubsystem(
        ip=ip,
        port=port,
        chord_node=chord_sub,
        storage_directory=data_path / "file_server",
    )

    await file_server.start()

    tag_server = TagServerSubsystem(
        ip=ip,
        port=port,
        chord_node=chord_sub,
        storage_directory=data_path / "tag_server",
    )

    await asyncio.sleep(3)
    log.info("Chord Node created")
    log.info("FileServer created")
    log.info("TagServer created")

    log.info(f"Creating the server at => tcp://{ip}:{port}")
    server = AsyncServer(
        ip=ip,
        port=port,
        handle_client_connection=lambda conn: handle_client(
            connection=conn,
            chord_subsystem=chord_sub,
            file_server=file_server,
            tag_server=tag_server,
        ),
        max_connections=500,
    )
    log.info("Server created")
    await server.run_server()


if __name__ == "__main__":
    app()
