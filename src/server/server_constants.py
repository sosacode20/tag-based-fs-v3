from basic_imports import *
from enum import Enum, unique

SERVER_SERVICE_NAME: str = "file.storage.server"
"""The name of the server service for use in the service discovery"""


class Subsystems(Enum):
    CHORD = b"CHORD"
    """The Chord Subsystem"""
    FILES = b"FILES"
    """The Files Subsystem"""
    TAGS = b"TAGS"
    """The Tags Subsystem"""
