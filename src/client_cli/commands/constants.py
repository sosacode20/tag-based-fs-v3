from enum import Enum, unique

CLIENT_SYSTEM = b"CLIENT"
"""The name of the client as an user of the file server"""
SERVER_SERVICE_NAME: str = "file.storage.server"
"""The name of the server service for use in the service discovery"""


class Commands(Enum):
    """Commands for server"""

    UPLOAD = b"UPLOAD"
    """Upload a file to the server with some tags"""
    DOWNLOAD = b"DOWNLOAD"
    """Download a file from the server by its name"""
    DELETE = b"DELETE"
    """Delete a file from the server by its name"""
    LIST = b"LIST"
    """List all files in the server by some filter tags"""
    ADD_TAGS = b"ADD_TAGS"
    """Add some tags to all files that match some tag-query in the server"""
    DELETE_TAGS = b"DELETE_TAGS"
    """Delete some tags from all files that match some tag-query in the server"""
