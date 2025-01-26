from basic_imports import *
import asyncio
from asyncio import AbstractEventLoop
import socket
from loguru import logger
import struct

from pydantic import BaseModel, Field, ValidationError, field_validator
from pydantic.networks import IPvAnyAddress


class MulticastGroup(BaseModel):
    """This class is used to store the multicast group address and port"""

    group: IPvAnyAddress = Field(
        description="The multicast group address",
        examples=["224.0.0.251"],
        default="224.0.0.251",
    )
    """The multicast group"""
    port: int = Field(
        gt=0,
        lt=65536,
        description="The multicast group port",
        default=5353,
    )
    """The port of the multicast group"""

    def to_json(self) -> str:
        """Convert the object to a JSON string"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str):
        """Create an instance of the class from a JSON string"""
        return cls.model_validate_json(json_str)


class MulticastHelper:
    """This class is used to send and receive messages from a multicast group"""

    def __init__(
        self,
        group: MulticastGroup,
        loop: asyncio.AbstractEventLoop = None,
        # enable_logs: bool = True,
    ):
        self.group: MulticastGroup = group
        """The multicast group"""
        self.socket: socket.socket = self.get_new_multicast_sock(self.group)
        """The multicast socket"""
        # self.loop = asyncio.get_event_loop()
        self.loop: AbstractEventLoop = self._get_loop() if loop is None else loop
        """The asyncio event loop"""
        self.logger = logger.bind(
            where="MulticastHelper", multicast_group=group.group, port=group.port
        )
        """The logger for this class"""
        # self.logger.disable()  # Disable the logger
        # if enable_logs:
        #     self.logger.enable()
        self.logger.info("Multicast Helper created")

    def _get_loop(self):
        """Get the asyncio event loop"""
        try:
            loop = asyncio.get_running_loop()
        except:
            loop = asyncio.get_event_loop()
        return loop

    def set_loop(self, loop: AbstractEventLoop):
        self.loop = loop

    @staticmethod
    def get_new_multicast_sock(group: MulticastGroup):
        """Create a new multicast socket, bind it to the multicast group and return it"""
        log = logger.bind(where="MulticastHelper", inside="get_new_multicast_socket")
        log.info("Creating a new multicast socket")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(
            socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2
        )  # Set the time to live of the message to 2
        sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
        )  # Allow the reuse of the address
        sock.setsockopt(
            socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0
        )  # Disable loopback. This means that the message will not be received by the sender
        sock.bind(((""), group.port))  # If an error occurs, uncomment this line
        # sock.bind((str(group.group), group.port))
        message_request = struct.pack(
            "4sl", socket.inet_aton(str(group.group)), socket.INADDR_ANY
        )
        log.info(
            f"The message request for joining a multicast group: {message_request}"
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, message_request)

        sock.setblocking(False)
        return sock

    async def send(self, message: bytes):
        """Send a message to the multicast group"""
        log = self.logger.bind(inside="send")
        try:
            # msg = json.dumps(message).encode("utf-8")
            log.bind(message=message, size=len(message)).info(f"Sending message...")
            await self.loop.sock_sendto(
                self.socket, message, (str(self.group.group), self.group.port)
            )
            # self.socket.sendto(message, (self.group.group, self.group.port))
            log.info("Message sent")
        except Exception as e:
            log.error(f"Error sending message: {e}")
            raise e

    async def receive(self, amount_b: int = 3 * 1024) -> bytes:
        """Receive a message from the multicast group"""
        log = self.logger.bind(inside="receive")
        try:
            log.info(f"Se quieren recibir '{amount_b}' bytes...")
            # data = self.socket.recv(amount_b)
            data = await self.loop.sock_recv(self.socket, amount_b)
            log.bind(message=data).info(f"Received Message.")
            return data
        except Exception as e:
            log.error(f"Error receiving message: {e}")
            raise e
