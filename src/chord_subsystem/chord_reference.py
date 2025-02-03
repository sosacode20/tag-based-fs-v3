from basic_imports import *
from .chord_interface import (
    ChordInterface,
    CHORD_SUBSYSTEM,
    OperationCodes,
    create_client,
    ChordResponse,
    ChordFailureTypes,
    ChordFailure,
    ChordSuccess,
    ChordData,
    ChordDataList,
)
from typing import Optional, Self
from loguru import logger
from async_sockets.async_connection import AsyncConnection, AsyncSocketDisconnected
import asyncio


class ChordNodeReference(ChordInterface):
    """This class represents a reference to a Chord Node.
    All the request to this reference, are made by RPC calls"""

    def __init__(
        self,
        ip: str,
        port: int,
        timeout: float = 5,  # In seconds
    ):
        super().__init__(ip, port)
        self.timeout: float = timeout
        """The timeout of each request in seconds"""
        self.logger = logger.bind(
            where="ChordNodeReference",
            this_node_ip=ip,
            this_node_port=port,
            this_node_id=id,
        )
        """This is the logger for this system"""

    @staticmethod
    def from_json(json_data: str | bytes) -> "ChordNodeReference":
        """Returns an instance of the ChordReference class given the information as json data"""
        chord_data = ChordData.from_json(json_data)
        return ChordNodeReference(ip=chord_data.ip, port=chord_data.port)

    async def send_data(
        self,
        data: list[bytes],
        expect_response: bool = True,
        timeout: Optional[float] = None,
    ) -> Optional[list[bytes]]:
        log = self.logger.bind(inside="send_data", expect_response=expect_response)
        log.info(f"Trying to send data => {data}")
        timeout = self.timeout if not timeout else timeout
        response: Optional[list[bytes]] = None
        try:
            log.debug(f"Trying to connect to => tcp://{self.ip}:{self.port}")
            client = await create_client(self.ip, self.port, my_logger=log)
            log.debug("Connection created")
            # NOTE: Messages MUST be of the form [FROM, TO] + data
            message = [CHORD_SUBSYSTEM, CHORD_SUBSYSTEM] + data
            # log.bind(message).debug("Sending the message")

            await asyncio.wait_for(
                client.send_multipart(message=message), timeout=timeout
            )
            log.debug("message successfully sended")
            if not expect_response:
                return ChordSuccess(result=[])

            log.debug("Waiting for response")
            b_response = await asyncio.wait_for(
                client.recv_multipart(), timeout=timeout
            )
            log.debug("Response received")
            response = b_response

        except asyncio.TimeoutError:
            log.error("The connection with server reached the timeout")
        except AsyncSocketDisconnected:
            log.error(f"The peer is disconnected")
        except BrokenPipeError as e:
            log.error(f"The connection was closed in an unexpected way")
        except Exception as e:
            log.error(f"An unknown exception occur =>\n{repr(e)}")
        finally:
            log.info("Closing connection with server")
            if client:
                client.close_connection()
            log.info("Connection closed")
            return response

    @property
    async def successor(self) -> Optional[Self]:
        log = self.logger.bind(
            inside="successor", node_address=f"{self.ip}:{self.port}"
        )
        log.info(f"Obtaining the successor node")

        operation = OperationCodes.GET_SUC.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation])
        log.debug("Response received")
        if response:
            assert (
                len(response) == 1
            ), "The response when asking for successors has more than 1 element in the multipart message"
            node = ChordData.from_json(response[0])
            return ChordNodeReference(node.ip, node.port, timeout=self.timeout)
        return None

    @property
    async def predecessor(self) -> Optional[Self]:
        log = self.logger.bind(
            inside="predecessor", node_address=f"{self.ip}:{self.port}"
        )
        log.info(f"Obtaining the successor node")

        operation = OperationCodes.GET_PRED.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation])
        log.debug("Response received")
        if response:
            assert (
                len(response) == 1
            ), "The response when asking for successors has more than 1 element in the multipart message"
            node = ChordData.from_json(response[0])
            return ChordNodeReference(node.ip, node.port, timeout=self.timeout)
        return None

    # async def get_successors(self, length: int) -> Optional[list[ChordInterface]]:
    async def get_successors(self, length: int) -> Optional[list[ChordInterface]]:
        log = self.logger.bind(
            inside="get_successors", node_address=f"{self.ip}:{self.port}"
        )
        log.info(f"Obtaining at most {length} successors from the node")

        operation = OperationCodes.GET_SUCCESSORS.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, length.to_bytes(length=1)])
        log.debug("Response received")
        if response:
            assert (
                len(response) == 1
            ), "The response when asking for successors has more than 1 element in the multipart message"
            nodes_data = ChordDataList.from_json(response[0])
            nodes = list(
                map(
                    lambda el: ChordNodeReference(el.ip, el.port, self.timeout),
                    nodes_data.nodes,
                )
            )
            return nodes
        return None

    async def get_predecessors(self, length: int) -> Optional[list[Self]]:
        log = self.logger.bind(
            inside="get_predecessors", node_address=f"{self.ip}:{self.port}"
        )
        log.info(f"Obtaining at most {length} successors from the node")

        operation = OperationCodes.GET_PREDECESSORS.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, length.to_bytes(length=1)])
        log.debug("Response received")
        if response:
            assert (
                len(response) == 1
            ), "The response when asking for successors has more than 1 element in the multipart message"
            nodes_data = ChordDataList.from_json(response[0])
            nodes = list(
                map(
                    lambda el: ChordNodeReference(el.ip, el.port, self.timeout),
                    nodes_data.nodes,
                )
            )
            return nodes
        return None

    async def find_successor(self, id: int) -> Optional[Self]:
        log = self.logger.bind(inside="find_successor", id_to_find=id)
        log.info(f"Finding successor of node with id => {hex(id)}")

        operation = OperationCodes.FIND_SUCCESSOR.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, id.to_bytes(length=32)])
        log.debug("Response received")
        if response:
            assert (
                len(response) == 1
            ), "The response for finding a successor should be a multipart message of length 1"
            node = ChordData.from_json(response[0])
            return ChordNodeReference(node.ip, node.port, self.timeout)
        return None

    async def find_predecessor(self, id: int) -> Optional[Self]:
        log = self.logger.bind(inside="find_predecessor", id_to_find=id)
        log.info(f"Finding predecessor of node with id => {hex(id)}")

        operation = OperationCodes.FIND_PREDECESSOR.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, id.to_bytes(length=32)])
        log.debug("Response received")
        if response:
            assert (
                len(response) == 1
            ), "The response for finding a predecessor should be a multipart message of length 1"
            node = ChordData.from_json(response[0])
            return ChordNodeReference(node.ip, node.port, self.timeout)
        return None

    async def closest_preceding_finger(self, id: int) -> Optional[Self]:
        log = self.logger.bind(inside="closest_preceding_finger", id_to_find=id)
        log.info(f"Finding closest preceding finger of node with id => {hex(id)}")

        operation = OperationCodes.CLOSEST_PRECEDING_FINGER.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, id.to_bytes(length=32)])
        log.debug("Response received")
        if response:
            assert (
                len(response) == 1
            ), "The response for finding the closest preceding finger should be a multipart message of length 1"
            node = ChordData.from_json(response[0])
            return ChordNodeReference(node.ip, node.port, self.timeout)
        return None

    async def notify(self, node: ChordInterface):
        log = self.logger.bind(inside="notify", node=node)
        log.info(f"Notifying to the node => {node}")

        operation = OperationCodes.NOTIFY.value
        log.debug("Waiting for a response of the node")
        await self.send_data(
            data=[operation, *node.to_multipart_message()],
            expect_response=False,
        )

    async def ping(self) -> bool:
        log = self.logger.bind(inside="ping")
        log.info(f"Doing a ping to this node")

        operation = OperationCodes.PING.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation], timeout=2)
        log.debug("Response received")
        return response != None
