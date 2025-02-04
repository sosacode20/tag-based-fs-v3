from basic_imports import *
from abc import ABC, abstractmethod
import hashlib
from typing import Self, Optional
from enum import Enum, unique
import socket
from async_sockets.async_connection import AsyncConnection
import asyncio
from loguru._logger import Logger
from pydantic import BaseModel, Field, IPvAnyAddress
from dataclasses import dataclass


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


CHORD_SUBSYSTEM = b"CHORD"
"""This is for identifying the subsystem"""


async def create_client(to_ip: str, to_port: int, my_logger: Logger) -> AsyncConnection:
    """Create an AsyncConnection that serves as a client connecting to a server"""
    log = my_logger.bind(inside="create_client", to_ip=to_ip, to_port=to_port)
    log.info("Creating the client")
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setblocking(False)
    address = (to_ip, to_port)
    log.info("Connecting to server")
    # client_socket.connect(address)
    loop = asyncio.get_running_loop()
    await loop.sock_connect(client_socket, address=address)
    log.info("Connection Successful")
    return AsyncConnection(connection_socket=client_socket, address=address)


@unique
class OperationCodes(Enum):
    # Operation codes
    FIND_SUCCESSOR = b"Find_S"
    FIND_PREDECESSOR = b"Find_P"
    GET_SUCCESSORS = b"Get_Successors"
    GET_PREDECESSORS = b"Get_Predecessors"
    GET_SUC = b"GET_SUC"
    GET_PRED = b"GET_PRED"
    NOTIFY = b"Notify"
    PING = b"Ping"
    CLOSEST_PRECEDING_FINGER = b"Closest_PF"
    JOIN = b"Join"


@dataclass
class ChordResponse[T]:
    """Base Class for all Responses for request to Chord Nodes"""

    pass


@unique
class ChordFailureTypes(Enum):
    """Possible failures for Chord Node requests"""

    DISCONNECTED = b"DISCONNECTED"
    """The Chord Node is disconnected"""
    UNKNOWN = b"UNKNOWN"
    """An unknown error"""
    TIMEOUT = b"TIMEOUT"
    """Response timeout reached"""


@dataclass
class ChordFailure(ChordResponse):
    """Represents a failure in a Chord Node request"""

    reason: ChordFailureTypes
    """The reason of the failure"""


@dataclass
class ChordSuccess[T](ChordResponse[T]):
    """Represents a success in a ChordNode request. It contains the result"""

    result: T
    """The result of the operation"""


class ChordData(BaseModel):
    ip: IPvAnyAddress = Field(description="The IP address of the service")
    """The IP address of the Chord Node"""
    port: int = Field(
        gt=0,
        lt=65536,
        description="The port number of the service",
    )
    """The port number of the Chord Node"""

    def to_json(self) -> str:
        """Converts the model to a json string"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str | bytes) -> "ChordData":
        """Create an instance of the class from a JSON string"""
        return cls.model_validate_json(json_str)


class ChordDataList(BaseModel):
    nodes: list[ChordData]
    """A list of Chord Nodes"""

    def to_json(self) -> str:
        """Converts the model to a json string"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str | bytes) -> "ChordDataList":
        """Create an instance of the class from a JSON string"""
        return cls.model_validate_json(json_str)


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

    def to_multipart_message(self) -> list[bytes]:
        """Creates a ZMQ multipart message that represents this node reference.
        It is a list of one element that is a ChordData object as json bytes"""
        # return [self.ip.encode(), self.port.to_bytes()]
        chord_data = ChordData(ip=self.ip, port=self.port)
        return [chord_data.to_json().encode()]

    def __str__(self):
        return f"ChordNode({self.ip}:{self.port}, id={self.id})"

    def __repr__(self):
        return f"ChordNode({self.ip}:{self.port}, id={self.id})"

    def __gt__(self, other):
        if isinstance(other, ChordInterface):
            return self.id > other.id
        raise Exception(
            "Trying to compare a ChordInterface with an object that is not of the correct type"
        )

    def __lt__(self, other):
        if isinstance(other, ChordInterface):
            return self.id < other.id
        raise Exception(
            "Trying to compare a ChordInterface with an object that is not of the correct type"
        )

    def __ge__(self, other):
        if isinstance(other, ChordInterface):
            return self.id >= other.id
        raise Exception(
            "Trying to compare a ChordInterface with an object that is not of the correct type"
        )

    def __le__(self, other):
        if isinstance(other, ChordInterface):
            return self.id <= other.id
        raise Exception(
            "Trying to compare a ChordInterface with an object that is not of the correct type"
        )

    def __eq__(self, value):
        if isinstance(value, ChordInterface):
            return self.id == value.id
        raise Exception(
            "Trying to compare a ChordInterface with an object that is not of the correct type"
        )

    def __ne__(self, value):
        if isinstance(value, ChordInterface):
            return self.id != value.id
        raise Exception(
            "Trying to compare a ChordInterface with an object that is not of the correct type"
        )

    def __hash__(self):
        return self.id

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
    async def get_successors(self, length: int) -> Optional[list[Self]]:
        """Get at most `length` successors of this node

        NOTE: The node MUST not give you it's last successor
        """
        pass

    @abstractmethod
    async def get_predecessors(self, length: int) -> Optional[list[Self]]:
        """Get at most `length` predecessors of this node

        NOTE: The node MUST not give you it's last successor
        """
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
    async def notify(self, node: Self) -> bool:
        """This function notify this node about another node that is potentially it's predecessor"""
        pass

    @abstractmethod
    async def ping(self) -> bool:
        """This function makes a ping to the node. It return True if the node is alive"""
        pass


def test_chord_responses(response: ChordResponse[list[bytes]]):
    match response:
        case ChordSuccess():
            result = response.result
            match result:
                case str():
                    print(f"ChordSuccess was a string with value => {result}")
                case list():
                    print(f"Success with a list of bytes => {result}")
                case _:
                    print(f"Success with Something unexpected")

        case ChordFailure():
            print(f"Was ChordFailure with reason {response.reason}")
        case _:
            print("Falls in default case")


def get_response() -> ChordResponse[list[bytes]]:
    return ChordSuccess(result="Success")


# test_chord_responses(response=ChordSuccess(result="Success"))
# test_chord_responses(response=ChordSuccess(result=[b"Success"]))
response = get_response()

test_chord_responses(response=response)
