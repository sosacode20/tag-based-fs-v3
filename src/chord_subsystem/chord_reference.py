from basic_imports import *
from chord_interface import (
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

    async def send_data(
        self,
        data: list[bytes],
        expect_response: bool = True,
    ) -> ChordResponse[list[bytes]]:
        log = self.logger.bind(inside="send_data", expect_response=expect_response)
        try:
            client = await create_client(self.ip, self.port, my_logger=log)
            response: ChordResponse = ChordFailure(reason=ChordFailureTypes.UNKNOWN)

            message = [CHORD_SUBSYSTEM] + data
            log.bind(message).debug("Sending the message")

            await asyncio.wait_for(
                client.send_multipart(message=message), timeout=self.timeout
            )
            log.debug("message successfully sended")
            if not expect_response:
                return ChordSuccess(result=[])

            log.debug("Waiting for response")
            b_response = await asyncio.wait_for(
                client.recv_multipart(), timeout=self.timeout
            )
            response = ChordSuccess(result=b_response)

        except asyncio.TimeoutError:
            log.error("The connection with server reached the timeout")
            response = ChordFailure(reason=ChordFailureTypes.TIMEOUT)
        # except asyncio.CancelledError:
        #     log.error("User pressed CTRL+C and CLI is stopping")
        #     log.info("Connection closed")
        #     raise
        except AsyncSocketDisconnected:
            log.error(f"The peer is disconnected")
            response = ChordFailure(reason=ChordFailureTypes.DISCONNECTED)
        except BrokenPipeError as e:
            log.error(f"The connection was closed in an unexpected way")
            response = ChordFailure(reason=ChordFailureTypes.DISCONNECTED)
        except Exception as e:
            log.error(f"An unknown exception occur =>\n{repr(e)}")
        finally:
            log.info("Closing connection with server")
            client.close_connection()
            log.info("Connection closed")
            return response

    @property
    async def successor(self) -> ChordResponse[Self]:
        log = self.logger.bind(
            inside="successor", node_address=f"{self.ip}:{self.port}"
        )
        log.info(f"Obtaining the successor node")

        operation = OperationCodes.GET_SUC.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation])
        log.debug("Response received")
        match response:
            case ChordSuccess():
                res = response.result
                assert (
                    len(res) == 1
                ), "The response when asking for successors has more than 1 element in the multipart message"
                nodes_data = ChordDataList.from_json(res[0])
                nodes = list(
                    map(
                        lambda el: ChordNodeReference(el.ip, el.port, self.timeout),
                        nodes_data,
                    )
                )
                return ChordSuccess(result=nodes)
            case ChordFailure():
                return response

    # async def get_successors(self, length: int) -> Optional[list[ChordInterface]]:
    async def get_successors(self, length: int) -> ChordResponse[list[ChordInterface]]:
        log = self.logger.bind(
            inside="get_successors", node_address=f"{self.ip}:{self.port}"
        )
        log.info(f"Obtaining at most {length} successors from the node")

        operation = OperationCodes.GET_SUCCESSORS.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, length.to_bytes(length=1)])
        log.debug("Response received")
        match response:
            case ChordSuccess():
                res = response.result
                assert (
                    len(res) == 1
                ), "The response when asking for successors has more than 1 element in the multipart message"
                nodes_data = ChordDataList.from_json(res[0])
                nodes = list(
                    map(
                        lambda el: ChordNodeReference(el.ip, el.port, self.timeout),
                        nodes_data,
                    )
                )
                return ChordSuccess(result=nodes)
            case ChordFailure():
                return response

    async def get_predecessors(self, length: int):
        log = self.logger.bind(
            inside="get_predecessors", node_address=f"{self.ip}:{self.port}"
        )
        log.info(f"Obtaining at most {length} successors from the node")

        operation = OperationCodes.GET_PREDECESSORS.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, length.to_bytes(length=1)])
        log.debug("Response received")
        match response:
            case ChordSuccess():
                res = response.result
                assert (
                    len(res) == 1
                ), "The response when asking for successors has more than 1 element in the multipart message"
                nodes_data = ChordDataList.from_json(res[0])
                nodes = list(
                    map(
                        lambda el: ChordNodeReference(el.ip, el.port, self.timeout),
                        nodes_data,
                    )
                )
                return ChordSuccess(result=nodes)
        return response

    async def find_successor(self, id: int):
        log = self.logger.bind(inside="find_successor", id_to_find=id)
        log.info(f"Finding successor of node with id => {hex(id)}")

        operation = OperationCodes.FIND_SUCCESSOR.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, str(id)])
        log.debug("Response received")
        match response:
            case ChordSuccess():
                res = response.result
                assert (
                    len(res) == 1
                ), "The response for finding a successor should be a multipart message of length 1"
                node = ChordData.from_json(res[0])
                return ChordSuccess(
                    result=ChordNodeReference(node.ip, node.port, self.timeout)
                )
        return response

    async def find_predecessor(self, id: int):
        log = self.logger.bind(inside="find_predecessor", id_to_find=id)
        log.info(f"Finding predecessor of node with id => {hex(id)}")

        operation = OperationCodes.FIND_PREDECESSOR.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, str(id)])
        log.debug("Response received")
        match response:
            case ChordSuccess():
                res = response.result
                assert (
                    len(res) == 1
                ), "The response for finding a predecessor should be a multipart message of length 1"
                node = ChordData.from_json(res[0])
                return ChordSuccess(
                    result=ChordNodeReference(node.ip, node.port, self.timeout)
                )
        return response

    async def closest_preceding_finger(self, id: int):
        log = self.logger.bind(inside="closest_preceding_finger", id_to_find=id)
        log.info(f"Finding closest preceding finger of node with id => {hex(id)}")

        operation = OperationCodes.CLOSEST_PRECEDING_FINGER.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, str(id)])
        log.debug("Response received")
        match response:
            case ChordSuccess():
                res = response.result
                assert (
                    len(res) == 1
                ), "The response for finding the closest preceding finger should be a multipart message of length 1"
                node = ChordData.from_json(res[0])
                return ChordSuccess(
                    result=ChordNodeReference(node.ip, node.port, self.timeout)
                )
        return response

    async def notify(self, node: ChordInterface):
        log = self.logger.bind(inside="notify", node=node)
        log.info(f"Notifying to the node => {node}")

        operation = OperationCodes.NOTIFY.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation, *node.to_multipart_message()])
        log.debug("Response received")
        return response

    async def ping(self):
        log = self.logger.bind(inside="ping")
        log.info(f"Doing a ping to this node")

        operation = OperationCodes.PING.value
        log.debug("Waiting for a response of the node")
        response = await self.send_data(data=[operation])
        log.debug("Response received")
        match response:
            case ChordSuccess():
                return True
            case _:
                return False
