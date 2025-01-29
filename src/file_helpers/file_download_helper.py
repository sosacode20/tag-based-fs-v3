from basic_imports import *
import aiofiles
from pathlib import Path
from .file_helper import calculate_checksum, FileMetadata
import os
from pydantic import BaseModel
from typing import Self
import asyncio
import hashlib

# The information to store in the metadata of the download helper are:
# - total_file_size => 8 bytes
# - downloaded_size => 8 bytes
# - file_version    => 4 bytes
# - file_checksum   => 32 bytes


class DownloadMetadata(BaseModel):
    file_size: int
    """The total expected size in bytes of the downloaded file"""
    downloaded_bytes: int
    """The number of downloaded bytes from the file"""
    file_version: int
    """The file version"""
    file_checksum: int
    """The expected checksum of the file when download is complete"""

    def to_bytes(self):
        total_size = self.file_size.to_bytes(length=8)
        downloaded_size = self.downloaded_bytes.to_bytes(length=8)
        file_version = self.file_version.to_bytes(length=4)
        file_checksum = self.file_checksum.to_bytes(length=32)
        return total_size + downloaded_size + file_version + file_checksum

    @classmethod
    def from_bytes(cls, metadata_in_bytes: bytes) -> Self:
        if len(metadata_in_bytes) != 52:
            raise Exception(
                f"Invalid metadata bytes. The number of bytes of metadata MUST be 52 bytes"
            )
        total_size = int.from_bytes(metadata_in_bytes[:8])
        downloaded_size = int.from_bytes(metadata_in_bytes[8:16])
        file_version = int.from_bytes(metadata_in_bytes[16:20])
        file_checksum = int.from_bytes(metadata_in_bytes[20:52])
        return DownloadMetadata(
            file_size=total_size,
            downloaded_bytes=downloaded_size,
            file_version=file_version,
            file_checksum=file_checksum,
        )

    def to_json(self):
        """Returns a JSON representation of this metadata"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json: bytes | str) -> Self:
        """Parse a Json representation of the `DownloadMetadata`"""
        return cls.model_validate_json(json_data=json)

    @classmethod
    def from_file_metadata(cls, file_metadata: FileMetadata) -> Self:
        """Creates a new `DownloadMetadata` object from a `FileMetadata. Like if it where a new download"""
        return DownloadMetadata(
            file_size=file_metadata.size,
            downloaded_bytes=0,
            file_version=file_metadata.version,
            file_checksum=file_metadata.checksum,
        )


class FileDownloadHelper:
    def __init__(
        self,
        file_name: str,
        storage_directory: Path,
        download_metadata: DownloadMetadata,
        overwrite_if_incompatible: bool = False,
    ):
        self.file_name: str = file_name
        """The original name of the file"""
        self.file_path: Path = storage_directory / f"{file_name}.part"
        """The path to the partial download of the file"""
        self.metadata: DownloadMetadata = download_metadata
        """The metadata of this file download"""
        self.overwrite_if_incompatible: bool = overwrite_if_incompatible
        # if not self.file_path.exists():
        #     metadata_in_file = await FileDownloadHelper.read_metadata(self.file_path)

    async def initialize(self):
        """Creates the .part file if not exists"""
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.touch()
        await self.append_metadata()

    @staticmethod
    def get_metadata_size() -> int:
        """Return the fixed size in bytes of the metadata"""
        # file_size + downloaded_bytes + file_version + file_checksum = 52 bytes
        return 8 + 8 + 4 + 32

    @staticmethod
    async def _read_metadata(file_path: Path) -> DownloadMetadata:
        """Read the metadata of the file"""
        if not file_path.exists():
            raise FileNotFoundError()
        file_size = file_path.stat().st_size
        metadata_size = FileDownloadHelper.get_metadata_size()
        if file_size < metadata_size:
            raise Exception(
                f"The file at '{file_path}' is not a valid partial downloaded file"
            )
        async with aiofiles.open(file_path, mode="rb") as file:
            await file.seek(-metadata_size, os.SEEK_END)
            metadata = await file.read(metadata_size)
            metadata = DownloadMetadata.from_bytes(metadata_in_bytes=metadata)
            return metadata

    async def read_metadata(self) -> DownloadMetadata:
        self.metadata = await FileDownloadHelper._read_metadata(self.file_path)
        return self.metadata

    async def remove_metadata(self):
        """Removes the metadata from the partial download file"""
        if not self.file_path.exists():
            raise FileNotFoundError()
        file_size = self.file_path.stat().st_size
        metadata_size: int = FileDownloadHelper.get_metadata_size()
        if file_size < metadata_size:
            raise Exception(
                f"The file at {self.file_path} has no metadata. It is not a valid partial downloaded file"
            )
        # with self.file_path.open(mode="wb") as file:
        #     # file.seek(-metadata_size, os.SEEK_END)
        #     await asyncio.to_thread(file.truncate, file_size - metadata_size)
        new_size: int = file_size - metadata_size
        async with aiofiles.open(self.file_path, mode="r+b") as file:
            # file.seek(0, os.SEEK_END)
            await file.truncate(new_size)

    async def append_metadata(self):
        """Append the metadata at the end of the file"""
        if not self.file_path.exists():
            raise FileNotFoundError()
        async with aiofiles.open(self.file_path, mode="+ab") as file:
            await file.write(self.metadata.to_bytes())

    async def get_number_bytes_to_download(self) -> int:
        """
        This method returns the number of bytes needed to complete the download
        This method expects that the file is created and with
        an associated metadata"""
        self.metadata = await self.read_metadata()
        return self.metadata.file_size - self.metadata.downloaded_bytes

    async def write_chunk(self, chunk: bytes):
        if not self.file_path.exists():
            raise FileNotFoundError()
        bytes_to_complete = await self.get_number_bytes_to_download()
        if len(chunk) > bytes_to_complete:
            raise Exception(
                f"The file {self.file_name} only need {bytes_to_complete} bytes to complete the download, but you tried to write {len(chunk)} bytes"
            )
        await self.remove_metadata()
        # metadata = self.metadata
        async with aiofiles.open(self.file_path, mode="ab") as file:
            # await file.seek(self.metadata.downloaded_bytes, os.SEEK_SET)  #
            await file.write(chunk)
        self.metadata.downloaded_bytes += len(chunk)
        await self.append_metadata()
        # await file.write(chunk + metadata.to_bytes())
        # await file.truncate()

    async def is_download_complete(self) -> bool:
        """Says if the download is complete"""
        self.metadata = await self.read_metadata()
        return self.metadata.downloaded_bytes == self.metadata.file_size

    async def calculate_checksum(self, has_metadata: bool = True) -> int:
        """Calculate the checksum of the downloaded file"""
        metadata_size: int = self.get_metadata_size()
        file_size = self.file_path.stat().st_size
        if has_metadata:
            file_size -= metadata_size
        read_bytes = 0
        hash_function = hashlib.sha256()
        async with aiofiles.open(self.file_path, mode="rb+") as file:
            to_read = min(4096, file_size - read_bytes)
            while to_read:
                hash_function.update(await file.read(to_read))
                read_bytes += to_read
                to_read = min(4096, file_size - read_bytes)
        return int.from_bytes(hash_function.digest())

    async def is_download_correct(self) -> bool:
        """Returns True if the download is complete and if the content has the correcto checksum"""
        if not await self.is_download_complete():
            return False
        actual_checksum = await self.calculate_checksum(has_metadata=True)
        expected_checksum = self.metadata.file_checksum
        return actual_checksum == expected_checksum

    async def save_completed_file(self, destination_folder: Path) -> bool:
        """Save the downloaded file if its completed, to the destination_folder"""
        # if not await self.is_download_correct():
        #     return False
        destination_folder.mkdir(parents=True, exist_ok=True)
        if not destination_folder.is_dir():
            raise Exception(
                f"The destination_folder '{destination_folder}' NEEDS to be a folder"
            )
        await self.remove_metadata()
        await asyncio.threads.to_thread(
            self.file_path.replace, destination_folder / self.file_name
        )
        return True
