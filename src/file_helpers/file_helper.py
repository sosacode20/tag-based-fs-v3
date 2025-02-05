from basic_imports import *
from pathlib import Path
from pydantic import BaseModel, Field
from datetime import datetime, timezone
import math
from enum import Enum
import hashlib
from typing import Self
import aiofiles


class SizeUnit(Enum):
    """The possible file size units"""

    Bytes = 0
    KB = 1
    MB = 2
    GB = 3
    TB = 4
    PB = 5


def calculate_checksum(file_path: Path) -> int:
    """
    Calculate the checksum of a file using the sha256 algorithm.

    Args:
        file_path (Path): The path to the file for which to calculate the checksum.

    Returns:
        int: The checksum value as an integer.

    Example:
    >>> checksum = calculate_checksum(Path("path/to/your/file.txt"))
    >>> print(checksum)
    """
    hash_function = hashlib.sha256()
    with file_path.open("rb") as f:
        while chunk := f.read(4096):
            hash_function.update(chunk)
    return int.from_bytes(hash_function.digest(), byteorder="big")


class FileMetadata(BaseModel):
    """Metadata of the file"""

    name: str = Field(min_length=1, max_length=255)
    """The name of the file"""
    size: int = Field(ge=0, lt=1024**4)
    """The file size in bytes"""
    version: int = Field(ge=0, default=0)
    """The version of the file"""
    checksum: int = Field(ge=0)
    """The checksum of the file. It is calculated using the sha256 algorithm"""

    def get_formatted_size(self) -> str:
        """Get the size and the size unit"""
        size = abs(self.size)
        unit_index = min(math.floor(math.log(size, 1024)), 5)
        new_size = size / 1024**unit_index
        return f"{new_size:.2f} {SizeUnit(unit_index).name}"

    def get_size(self, unit: SizeUnit = SizeUnit.Bytes) -> str:
        """Get the size in the specified unit"""
        return f"{self.size / 1024**int(unit):.2f} {str(unit)}"

    def to_json(self):
        """Convert the metadata to a JSON string"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str | bytes) -> Self:
        """Create an instance of the class from a JSON string"""
        return cls.model_validate_json(json_str)

    def __eq__(self, value):
        if isinstance(value, FileMetadata):
            return (
                self.name == value.name
                and self.size == value.size
                and self.checksum == value.checksum
            )
        return False

    def __ne__(self, value):
        return not self.__eq__(value)

    def __hash__(self):
        return int(hashlib.sha1(self.name.encode()).hexdigest(), 16)


class FileHelper(object):
    """This class is a helper for obtaining chunks of a file. Also, for obtaining metadata of the same"""

    def __init__(self, file_path: Path):
        self.file_path: Path = file_path
        """The path of the file"""

    def get_file_size(self) -> int:
        """Returns the file size in bytes"""
        return self.file_path.stat().st_size

    def get_metadata(self) -> FileMetadata:
        """Returns the file metadata."""
        stats = self.file_path.stat()
        metadata = FileMetadata(
            name=self.file_path.name,
            size=stats.st_size,
            checksum=calculate_checksum(self.file_path),
        )
        return metadata

    def valid_chunk_request(self, offset: int, chunk_size: int) -> bool:
        """
        Validates if the requested chunk is within the bounds of the file.

        This method checks if the requested chunk starting at the given 'offset' and of size 'chunk_size'
        is within the bounds of the file. If the offset exceeds the file size, the method returns False.

        Args:
            offset (int): The starting position in the file from which to read the chunk (in bytes).
            chunk_size (int): The size of the chunk to be read (in bytes).

        Returns:
            bool: True if the requested chunk is within the bounds of the file, False otherwise.

        Example:
        >>> helper = FileHelper('path/to/your/file.txt')
        >>> # Check if a chunk of size 1024 bytes starting at the byte offset 32 is valid
        >>> is_valid = helper.valid_chunk_request(32, 1024)
        >>> print(is_valid)
        """
        size: int = self.get_file_size()
        return offset < size and offset + chunk_size <= size

    def get_chunk(self, offset: int, chunk_size: int) -> bytes:
        """
        Retrieves a chunk of data from the file starting at the specified offset.

        This method reads a single chunk of data of size 'chunk_size' from the file, beginning
        at the given 'offset'. If the offset exceeds the file size, an exception is raised.
        The method ensures that the file is opened in binary read mode.

        Args:
            offset (int): The starting position in the file from which to read the chunk (in bytes).
            chunk_size (int): The size of the chunk to be read (in bytes).

        Returns:
            bytes: The data of the specified chunk.

        Raises:
            Exception: If the specified offset exceeds the total size of the file.

        Example:
        >>> helper = FileHelper('path/to/your/file.txt')
        >>> # Give me a chunk of at most 1024 bytes in size starting at the byte offset 32
        >>> # An error will be raised if the offset is greater than the file size
        >>> chunk = helper.get_chunk(32, 1024)
        >>> print(chunk)
        """
        size: int = self.get_file_size()
        if offset > size:
            raise Exception(
                f"The file '{self.file_path}' has {size} bytes and the bytes offset is {offset} which is greater than the size of the file."
            )
        with self.file_path.open("rb") as file:
            file.seek(
                offset
            )  # if you want to read from the end, then `file.seek(-offset, os.SEEK_END)`
            fragment = file.read(chunk_size)
        return fragment

    async def get_chunk_async(self, offset: int, chunk_size: int) -> bytes:
        """
        Retrieves a chunk of data from the file starting at the specified offset asynchronously.

        This method reads a single chunk of data of size 'chunk_size' from the file, beginning
        at the given 'offset'. If the offset exceeds the file size, an exception is raised.
        The method ensures that the file is opened in binary read mode.

        Args:
            offset (int): The starting position in the file from which to read the chunk (in bytes).
            chunk_size (int): The size of the chunk to be read (in bytes).

        Returns:
            bytes: The data of the specified chunk.

        Raises:
            Exception: If the specified offset exceeds the total size of the file.

        Example:
        >>> helper = FileHelper('path/to/your/file.txt')
        >>> # Give me a chunk of at most 1024 bytes in size starting at the byte offset 32
        >>> # An error will be raised if the offset is greater than the file size
        >>> chunk = await helper.get_chunk_async(32, 1024)
        >>> print(chunk)
        """
        size: int = self.get_file_size()
        if offset > size:
            raise Exception(
                f"The file '{self.file_path}' has {size} bytes and the bytes offset is {offset} which is greater than the size of the file."
            )
        async with aiofiles.open(self.file_path, "rb") as file:
            await file.seek(offset)
            fragment = await file.read(chunk_size)
        return fragment

    def get_chunks(self, offset: int, chunk_size: int, n_chunks: int) -> list[bytes]:
        """
        Retrieves a specified number of chunks of data from the file, starting from a given offset.

        This method reads a maximum of 'n_chunks' from the file, each of size 'chunk_size',
        beginning at the specified 'offset'. If the offset is beyond the file size, an exception
        is raised. The method ensures that it does not attempt to read beyond the end of the file.

        Args:
            offset (int): The starting position in the file from which to read chunks (in bytes).
            chunk_size (int): The size of each chunk to be read (in bytes).
            n_chunks (int): The maximum number of chunks to retrieve.

        Returns:
            list[bytes]: A list containing the retrieved chunks of data,
                         which may contain fewer than 'n_chunks' if the end of the file is reached.

        Raises:
            Exception: If the specified offset exceeds the total size of the file.

        Example:
        >>> helper = FileHelper('path/to/your/file.txt')
        >>> # Get 5 chunks of size 1024 bytes starting at the byte offset 32
        >>> # An error will be raised if the offset (32 in this example) is greater than the total size of the file
        >>> chunks = helper.get_chunks(32, 1024, 5)
        >>> for i, chunk in enumerate(chunks):
        >>>     print(f"Chunk {i}: {chunk}")
        """
        size: int = self.get_file_size()
        if offset > size:
            raise Exception(
                f"The file '{self.file_path}' has {size} bytes and the bytes offset is {offset} which is greater than the size of the file."
            )
        with self.file_path.open("rb") as file:
            file.seek(offset)
            fragment = file.read(n_chunks * chunk_size)
            fragments = []
            for i in range(n_chunks):
                if offset + i * chunk_size > size:
                    break
                fragments.append(fragment[i * chunk_size : (i + 1) * chunk_size])
        return fragments

    async def get_chunks_async(
        self, offset: int, chunk_size: int, n_chunks: int
    ) -> list[bytes]:
        """
        Retrieves a specified number of chunks of data from the file, starting from a given offset asynchronously.

        This method reads a maximum of 'n_chunks' from the file, each of size 'chunk_size',
        beginning at the specified 'offset'. If the offset is beyond the file size, an exception
        is raised. The method ensures that it does not attempt to read beyond the end of the file.

        Args:
            offset (int): The starting position in the file from which to read chunks (in bytes).
            chunk_size (int): The size of each chunk to be read (in bytes).
            n_chunks (int): The maximum number of chunks to retrieve.

        Returns:
            list[bytes]: A list containing the retrieved chunks of data,
                         which may contain fewer than 'n_chunks' if the end of the file is reached.

        Raises:
            Exception: If the specified offset exceeds the total size of the file.

        Example:
        >>> helper = FileHelper('path/to/your/file.txt')
        >>> # Get 5 chunks of size 1024 bytes starting at the byte offset 32
        >>> # An error will be raised if the offset (32 in this example) is greater than the total size of the file
        >>> chunks = await helper.get_chunks_async(32, 1024, 5)
        >>> for i, chunk in enumerate(chunks):
        >>>     print(f"Chunk {i}: {chunk}")
        """
        size: int = self.get_file_size()
        if offset > size:
            raise Exception(
                f"The file '{self.file_path}' has {size} bytes and the bytes offset is {offset} which is greater than the size of the file."
            )
        async with aiofiles.open(self.file_path, "rb") as file:
            await file.seek(offset)
            fragment = await file.read(n_chunks * chunk_size)
            fragments = []
            for i in range(n_chunks):
                if offset + i * chunk_size > size:
                    break
                fragments.append(fragment[i * chunk_size : (i + 1) * chunk_size])
        return fragments

    def get_index_chunk(self, chunk_size: int, chunk_index: int) -> bytes:
        """
        Retrieves a specific chunk of data from the file based on the provided chunk size and index.

        This method calculates the total number of chunks in the file by dividing the file size by the
        specified chunk size. It raises an exception if the requested chunk index is out of bounds.
        If valid, it seeks to the appropriate position in the file and reads the specified chunk of data.

        Args:
            chunk_size (int): The size of each chunk in bytes.
            chunk_index (int): The index of the chunk to retrieve (0-based).

        Returns:
            bytes: The data of the specified chunk.

        Raises:
            Exception: If the requested chunk index exceeds the total number of available chunks.

        Example:
        >>> helper = FileHelper('path/to/your/file.txt')
        >>> # Give me the 3rd chunk of size 1024 bytes from the file. If the file has less than 3 chunks of size 1024, then an error will be raised
        >>> # NOTE: This method gives you a chunk of 'chunk_size' bytes if you are NOT asking for the last chunk. The last chunk could have less than 'chunk_size' bytes
        >>> chunk = helper.get_index_chunk(1024, 3)
        >>> print(chunk)
        """
        with self.file_path.open("rb") as file:
            size: int = self.get_file_size()
            total_chunks = math.ceil(size / chunk_size)
            if chunk_index >= total_chunks:
                raise Exception(
                    f"The file '{self.file_path.name}' only has {total_chunks} of at most {chunk_size} bytes. You are asking for the {chunk_index} chunk which doesn't exist"
                )
            file.seek(chunk_size * chunk_index)
            return file.read(chunk_size)

    async def get_index_chunk_async(self, chunk_size: int, chunk_index: int) -> bytes:
        """
        Retrieves a specific chunk of data from the file based on the provided chunk size and index asynchronously.

        This method calculates the total number of chunks in the file by dividing the file size by the
        specified chunk size. It raises an exception if the requested chunk index is out of bounds.
        If valid, it seeks to the appropriate position in the file and reads the specified chunk of data.

        Args:
            chunk_size (int): The size of each chunk in bytes.
            chunk_index (int): The index of the chunk to retrieve (0-based).

        Returns:
            bytes: The data of the specified chunk.

        Raises:
            Exception: If the requested chunk index exceeds the total number of available chunks.

        Example:
        >>> helper = FileHelper('path/to/your/file.txt')
        >>> # Give me the 3rd chunk of size 1024 bytes from the file. If the file has less than 3 chunks of size 1024, then an error will be raised
        >>> # NOTE: This method gives you a chunk of 'chunk_size' bytes if you are NOT asking for the last chunk. The last chunk could have less than 'chunk_size' bytes
        >>> chunk = await helper.get_index_chunk_async(1024, 3)
        >>> print(chunk)
        """
        async with aiofiles.open(self.file_path, "rb") as file:
            size: int = self.get_file_size()
            total_chunks = math.ceil(size / chunk_size)
            if chunk_index >= total_chunks:
                raise Exception(
                    f"The file '{self.file_path.name}' only has {total_chunks} of at most {chunk_size} bytes. You are asking for the {chunk_index} chunk which doesn't exist"
                )
            await file.seek(chunk_size * chunk_index)
            return await file.read(chunk_size)
