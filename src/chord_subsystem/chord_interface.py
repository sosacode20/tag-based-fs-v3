from basic_imports import *
from abc import ABC, abstractmethod
import hashlib
from typing import Self, Optional
from enum import Enum, unique


def getShaRepr(data: str) -> int:
    """Function to hash a string using SHA-1 and return its integer representation"""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)


def get_id_of_node(ip: str, port: int) -> int:
    """Returns the ID of the Chord Node associated with the IP:Port address"""
    return getShaRepr(f"{ip}:{port}")


def in_between(id: int, start: int, end: int) -> bool:
    """Check (In a ring) if the id is in between the start and end"""
    # TODO: Remember the ring is circular, so the end can be less than the start
    if start < end:
        return start < id <= end
    return start < id or id <= end


CHORD_SUBSYSTEM = b"Chord"
"""This is for identifying the subsystem"""


@unique
class OperationCodes(Enum):
    # Operation codes
    FIND_SUCCESSOR = b"Find_S"
    FIND_PREDECESSOR = b"Find_P"
    GET_SUCCESSOR = b"Get_S"
    GET_PREDECESSOR = b"Get_P"
    NOTIFY = b"Notify"
    PING = b"Ping"
    CLOSEST_PRECEDING_FINGER = b"Closest_PF"
    JOIN = b"Join"


class ChordInterface(ABC):
    """Base class for all chord functionalities implementation"""

    def __init__(self, ip: str, port: int):
        super().__init__()
        self.ip: str = ip
        """The ip of this node"""
        self.port: int = port
        """The port of this node"""
        self.id: int = get_id_of_node(ip, port)
        """The id of this node in the Chord Ring"""

    def to_zmq_multipart(self) -> list[bytes]:
        """Creates a ZMQ multipart message that represents this node reference"""
        return [self.ip.encode(), self.port.to_bytes()]

    def __str__(self):
        return f"ChordNode({self.ip}:{self.port})"

    def __repr__(self):
        return f"ChordNode({self.ip}:{self.port})"

    @property
    @abstractmethod
    async def successor(self) -> Optional[Self]:
        """Gets the successor node."""
        pass

    @property
    @abstractmethod
    async def predecessor(self) -> Optional[Self]:
        """Gets the predecessor node."""
        pass

    @abstractmethod
    async def find_successor(self, id: int) -> Optional[Self]:
        """Finds the `ChordNode` of the immediate successor of the `id`"""
        pass

    @abstractmethod
    async def find_predecessor(self, id: int) -> Optional[Self]:
        """Finds the `ChordNode` of the immediate predecessor of the `id`"""
        pass

    @abstractmethod
    async def closest_preceding_finger(self, id: int) -> Optional[Self]:
        """Find the closest preceding finger preceding the `id`"""
        pass

    @abstractmethod
    async def notify(self, node: Self) -> None:
        """This function notify this node about another node that is potentially it's predecessor"""
        pass

    @abstractmethod
    async def ping(self) -> bool:
        """This function makes a ping to the node. It return True if the node is alive"""
        pass
