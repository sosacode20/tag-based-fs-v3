from pydantic import BaseModel
from file_helpers.file_helper import FileMetadata
from chord_subsystem.chord_interface import getShaRepr, in_between
from typing import Optional, Self
from pathlib import Path
import aiofiles


def get_file_id(file_name: str) -> str:
    """
    Get the file id for the given file name

    :param file_name: The name of the file
    :return: The file id
    """
    return getShaRepr(file_name)


class FileServerFilesMeta(BaseModel):
    files: list[FileMetadata] = []
    """The list of files that this server has"""

    # async def save_as_file(self, )

    def add_files(self, files: list[FileMetadata]) -> None:
        """
        Add a list of files to the list of files

        :param files: The files to add
        """
        self.files.extend(files)
        self.files = list(set(self.files))

    def remove_files(self, file_names: list[str]) -> None:
        """
        Remove a file from the list of files

        :param file_name: The name of the file to remove
        """
        self.files = [file for file in self.files if file.file_name not in file_names]

    def get_file(self, file_name: str) -> Optional[FileMetadata]:
        """
        Get the file metadata for the given file name

        :param file_name: The name of the file
        :return: The file metadata if it exists, None otherwise
        """
        for file in self.files:
            if file.file_name == file_name:
                return file
        return None

    def has_file(self, file_meta: FileMetadata) -> bool:
        """
        True if the file exists in the database
        """
        return file_meta in self.files

    def get_files_in_and_out_range(
        self,
        range: tuple[int, int],
    ) -> tuple[list[FileMetadata], list[FileMetadata]]:
        """
        Get all the files in the given range

        :param range: The range to get the files from. The range is in the form (start, end]
        :return: A tuple of two lists. The first list contains all the files in the range, the second list contains all the files outside the range
        """
        in_range: list[FileMetadata] = []
        out_range: list[FileMetadata] = []
        for file in self.files:
            file_id = get_file_id(file.name)
            if in_between(file_id, *range):
                in_range.append(file)
            else:
                out_range.append(file)
        return in_range, out_range
