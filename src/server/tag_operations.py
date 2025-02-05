from basic_imports import *
from typing import Optional
from pathlib import Path
from loguru._logger import Logger
from loguru import logger
from async_sockets.async_connection import AsyncConnection
from file_helpers.file_download_helper import DownloadMetadata, FileDownloadHelper
from file_helpers.file_helper import FileMetadata, FileHelper
from .server_constants import Commands
import json
import asyncio


async def list_files_with_tag_query(
    connection: AsyncConnection,
    tag: str,
    metadata_folder: Path,
):
    """List all the files with the given tag"""
    pass
