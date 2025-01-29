from basic_imports import *
from logging_helper.logging_utils import LogFilters, custom_log_format
from cyclopts import App, Parameter
from loguru import logger
from loguru._logger import Logger
from pathlib import Path
from file_helpers.file_download_helper import DownloadMetadata, FileDownloadHelper
from file_helpers.file_helper import FileMetadata, FileHelper
import os
from service_discovery.service_watcher import ServiceWatcher, ServiceAnnouncement
from socket import gethostbyname, gethostname


def get_data_base_folder() -> Path:
    """Returns the Path where the data should be stored"""
    folder_path = os.getenv("DATA_FOLDER")
    if folder_path:
        return Path(folder_path)
    return Path(__file__).parents[2] / "data"


def get_default_filter() -> LogFilters:
    filter = LogFilters()
    filter.add_filter(
        where="tag_file_server", inside=[]
    )  # TODO: Fill the `inside` list
    return filter


def get_ip():
    return


def get_service_watcher() -> ServiceWatcher:
    return ServiceWatcher(
        services_to_announce=[
            ServiceAnnouncement(
                service="file.storage.server",
                ip=gethostbyname(gethostname()),
                port=5000,
            )
        ]
    )


def start_server():
    pass


default_filter = get_default_filter()
data_path = get_data_base_folder()
app = App("A CLI app for creating a file server")
logger.remove()
logger.add(sys.stdout, filter=default_filter, colorize=True, backtrace=True)
logger.add(
    data_path / "logs" / "server_only.log",
    filter=default_filter,
    format=custom_log_format,
    backtrace=True,
    rotation="50 MB",
    # compression="zip",
    serialize=True,
)
my_logger = logger.bind(where="file_server.py")
watcher = get_service_watcher()
