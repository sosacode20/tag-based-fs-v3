from pydantic import BaseModel
import hashlib


class FileWithTags(BaseModel):
    """A file with associated tags"""

    file_name: str
    """The name of the file"""
    tags: list[str]
    """The list of tags associated with the file"""

    def add_tags(self, new_tags: list[str]):
        """Add new tags to the file"""
        self.tags = list(set(self.tags + new_tags))

    def remove_tags(self, tags_to_remove: list[str]):
        """
        Remove tags from the file
        """
        self.tags = [tag for tag in self.tags if tag not in tags_to_remove]

    def __eq__(self, value):
        if not isinstance(value, FileWithTags):
            return False
        return self.file_name == value.file_name

    def __hash__(self):
        return int(hashlib.sha1(self.name.encode()).hexdigest(), 16)


class TagServerMetadata(BaseModel):
    """The metadata for the tag server"""

    files_with_tags: list[FileWithTags] = []
    """The list of files with tags"""

    def add_file_with_tags(self, file_with_tags: FileWithTags):
        """
        Add a file with tags to the list of files with tags

        :param file_with_tags: The file with tags to add
        """
        self.files_with_tags.append(file_with_tags)

    def remove_file_with_tags(self, file_name: str):
        """
        Remove a file with tags from the list of files with tags

        :param file_name: The name of the file to remove
        """
        self.files_with_tags = [
            file for file in self.files_with_tags if file.file_name != file_name
        ]

    def get_file_with_tags(self, file_name: str) -> FileWithTags:
        """
        Get the file with tags for the given file name

        :param file_name: The name of the file
        :return: The file with tags if it exists, None otherwise
        """
        for file in self.files_with_tags:
            if file.file_name == file_name:
                return file
        return None

    def get_files_with_tag(self, tag: str) -> list[FileWithTags]:
        """
        Get all the files with the given tag

        :param tag: The tag to search for
        :return: A list of files with the given tag
        """
        return [file for file in self.files_with_tags if tag in file.tags]
