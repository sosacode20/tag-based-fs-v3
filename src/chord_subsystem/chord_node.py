from .chord_interface import (
    ChordInterface,
    in_between,
    get_id_of_node,
    CHORD_SUBSYSTEM,
    OperationCodes,
    ChordData,
    ChordDataList,
)
from .chord_reference import ChordNodeReference
from .successor_list import SuccessorList, PredecessorList
from loguru import logger
import asyncio
from typing import Optional
from service_discovery.service_watcher import ServiceWatcher, Peer
from server.server_constants import SERVER_SERVICE_NAME
import zmq
from zmq.asyncio import Poller

CHORD_SERVICE_NAME: str = "CHORD_NODE"
"""The name of the service that the Chord Node will use"""

CHORD_BACKEND_NAME: str = "CHORD_BACK"
"""The name of the CHORD service for backend_service"""


class ChordNode(ChordInterface):
    def __init__(
        self,
        ip: str,
        port: int,
        watcher: ServiceWatcher,
        m: int = 160,
    ):
        super().__init__(ip, port)
        # self.ref: ChordInterface = ChordNodeReference(self.ip, self.port)
        # """The Chord reference to this node. Use this to make calls to yourself"""
        self.m: int = m
        """The number of bits in the hash/key space"""
        self.finger: list[ChordInterface] = [self] * self.m
        """Finger Table of this `ChordNode`. It includes the actual node as the first reference"""
        self.successors: SuccessorList = SuccessorList(
            capacity=3,
            primary_node=self,
        )
        """The successor list"""
        self.predecessors: PredecessorList = PredecessorList(
            capacity=3,
            primary_node=self,
        )
        """The predecessor list"""
        self.logger = logger.bind(where="ChordNode", this_node=self)
        """The logger of this class"""
        self.watcher: ServiceWatcher = watcher
        """The Service Watcher to use"""
        self.zmq_context: zmq.SyncContext = zmq.SyncContext.instance()
        """The zmq context for creating new sockets"""

    def start(self):
        """Start the node. This function MUST be called from a new thread"""
        asyncio.run(self.run())

    async def run(self):
        """This is the function that creates all the coroutines to run the node"""
        log = self.logger.bind(inside="run")
        log.info("Starting the ChordNode parallel methods")
        await asyncio.gather(
            # self.start_server(),
            self.stabilize(),
            self.fix_fingers(),
            self.check_predecessor(),
            self.where_to_join(),
        )

    async def get_range(self) -> tuple[int, int]:
        """Returns the range of keys for which we are responsible for.

        The range is of the form (start, end]"""
        pred = await self.predecessor
        return pred.id, self.id

    # region Chord RPC methods

    @property
    async def successor(self) -> Optional[ChordInterface]:
        return await self.successors.get_successor()

    @property
    async def predecessor(self) -> Optional[ChordInterface]:
        return await self.predecessors.get_predecessor()

    async def get_successors(self, length: int) -> Optional[list[ChordInterface]]:
        return await self.successors.get_successors(length=length)

    async def get_predecessors(self, length: int) -> Optional[list[ChordInterface]]:
        return await self.predecessors.get_predecessors(length=length)

    async def find_successor(self, id: int) -> Optional[ChordInterface]:
        log = self.logger.bind(inside="find_successor", id=id)
        log.info(f"Finding the successor of {id}")
        node = await self.find_predecessor(id)
        if node is None:
            log.warning(
                f"`find_predecessor` returned None. Exiting the function of `find_successor` with None"
            )
            return None
        log.info(f"Found the predecessor of {id} = {node}")
        succ = await node.successor
        if succ is None:
            log.warning(
                f"Successor of node {node} is None. Exiting the function of `find_successor` with None"
            )
            return None
        log.debug(f"Successor of {node} = {succ}")
        return succ

    async def find_predecessor(self, id: int) -> Optional[ChordInterface]:
        log = self.logger.bind(inside="find_predecessor", id=id)
        log.info(f"Finding the predecessor of {id}")
        node: ChordInterface = self
        succ: ChordInterface = await self.successor
        if succ == node:
            log.info(f"Successor of node {node} is the node itself")
            return node
        log.debug(f"Initial node: {node} with successor: {succ}")
        while not in_between(id, node.id, succ.id):
            log.debug(f"Searching for the predecessor of {id} in the node {node}")
            node = succ
            succ = await node.successor
            if succ is None:
                log.warning(
                    f"Successor of node {node} is None. Exiting the loop. THE NODE IS DEAD"
                )
                return None  # TODO: Check if this is the correct way to handle this

        log.info(f"Found the predecessor of {id} = {node}")
        return node

    async def closest_preceding_finger(self, id: int) -> Optional[ChordInterface]:
        log = self.logger.bind(inside="closest_preceding_finger", search_id=id)
        log.info(f"Searching for the closest preceding finger of the id = {id}")
        for i in range(self.m - 1, -1, -1):
            finger = self.finger[i]
            if in_between(finger.id, self.id, id):
                # log.info("Closest preceding finger found => ")
                return finger
        return self

    async def notify(self, node: ChordInterface) -> bool:
        # return await super().notify(node)
        self.predecessors.add(node)
        return True

    async def ping(self) -> bool:
        return True

    def is_ready(self) -> bool:
        return len(self.successors) >= 3

    # endregion

    # region Chord Parallel methods

    async def where_to_join(self):
        log = self.logger.bind(inside="where_to_join")
        while True:
            if len(self.successors) > 0:
                await asyncio.sleep(5)
                continue
            log.info("Searching for a peer to connect")
            watcher = self.watcher
            peers: list[Peer] = watcher.get_services(service_name=SERVER_SERVICE_NAME)
            if len(peers) == 0:
                log.info("No peers found. Waiting for 3 seconds")
                await asyncio.sleep(3)
                continue
            available_peers: list[ChordNodeReference] = list(
                map(
                    lambda peer: ChordNodeReference(
                        str(peer.service.ip), peer.service.port
                    ),
                    peers,
                )
            )
            log.bind(peers=available_peers).info(
                f"Found {len(available_peers)} peers => {available_peers}"
            )
            peer_to_connect = min(available_peers, key=lambda peer: peer.id)
            log.bind(peer_to_connect=peer_to_connect).info(
                f"Starting join operation of this node {self} with the peer => {peer_to_connect}"
            )
            await self.join(peer_to_connect)
            await asyncio.sleep(2)

    async def join(self, node: ChordNodeReference):
        """Join the ring with the node"""
        # TODO: Implement this
        log = self.logger.bind(inside="join", node=node)
        log.info(f"Joining the ring with the node {node}")
        log.info(f"Asking for the successor of my id = {self.id}")
        succ = await node.find_successor(self.id)
        if succ:
            if succ == self:
                log.info(
                    f"The successor of my id = {self.id} is myself. That means that my predecessor is my successor"
                )
                pred = await self.predecessor
                if pred == self:
                    log.warning(
                        f"The predecessor of my id = {self.id} is myself. It should not happen"
                    )
                    return
                log.info(f"Setting my predecessor as my successor => {pred}")
                self.successors.add(pred)
                await pred.notify(self)
            else:
                log.info(f"The successor of my id = {self.id} is {succ}")
                self.successors.add(succ)
                await succ.notify(self)
        else:
            log.warning(
                f"The successor of my id = {self.id} is None. Exiting the function"
            )

    async def stabilize(self):
        """Maintain the successor list"""
        # TODO: Implement this
        log = self.logger.bind(inside="stabilize")
        while True:
            await asyncio.sleep(3)
            log.info("Stabilizing the successor list")
            log.debug(
                f"Before stabilization, the successors where => {self.successors}"
            )
            await self.successors.update()
            log.debug(f"After stabilization, the successors where => {self.successors}")

    async def fix_fingers(self):
        """Periodically refresh the finger table"""
        # TODO: Implement this
        pass

    async def check_predecessor(self):
        """Check if the predecessor is alive"""
        log = self.logger.bind(inside="check_predecessor")
        while True:
            await asyncio.sleep(3)
            log.info("Stabilizing the predecessors")
            log.debug(
                f"Before stabilization, the predecessors where => {self.predecessors}"
            )
            await self.predecessors.update()
            log.debug(
                f"After stabilization, the predecessors are => {self.predecessors}"
            )
            log.info("Predecessors Stabilization completed")

    # endregion

    async def handle_request(self, request: list[bytes]) -> Optional[list[bytes]]:
        """Handle a request to the Chord Node"""
        log = self.logger.bind(inside="handle_request")
        log.info("Handling a new request for Chord")
        # assert (
        #     len(request) >= 3
        # ), "Request to Chord MUST be of the form [FROM_WHERE, CHORD, *rest]"
        # request = request[2:]
        assert len(request) >= 1, "Empty request for ChordNode"
        match request:
            case OperationCodes.GET_SUC.value,:
                log.info("The request is for obtaining the successor of this node")
                succ = await self.successor
                log.info(f"Returning the successor => {succ}")
                return succ.to_multipart_message()

            case OperationCodes.GET_PRED.value,:
                log.info("The request is for obtaining the predecessor of this node")
                pred = await self.predecessor
                log.info(f"Returning the predecessor => {pred}")
                return pred.to_multipart_message()

            case OperationCodes.FIND_SUCCESSOR.value, id:
                id = int.from_bytes(id, byteorder="big")
                log.info(f"The request is for finding the successor of the id = {id}")
                succ = await self.find_successor(id)
                log.info(f"Found the successor of {id} = {succ}")
                if succ:
                    return succ.to_multipart_message()

            case OperationCodes.FIND_PREDECESSOR.value, id:
                id = int.from_bytes(id, byteorder="big")
                log.info(f"The request is for finding the predecessor of the id = {id}")
                pred = await self.find_predecessor(id)
                log.info(f"Found the predecessor of {id} = {pred}")
                if pred:
                    return pred.to_multipart_message()

            case OperationCodes.GET_SUCCESSORS.value, length:
                length = int.from_bytes(length, byteorder="big")
                log.info(
                    f"The request is for obtaining {length} successors of this node"
                )
                successors = await self.get_successors(length)
                successors = [ChordData(ip=s.ip, port=s.port) for s in successors]
                log.info(f"Successors of this node are => {successors}")
                return [ChordDataList(nodes=successors).to_json().encode()]

            case OperationCodes.GET_PREDECESSORS.value, length:
                length = int.from_bytes(length, byteorder="big")
                log.info(
                    f"The request is for obtaining {length} predecessors of this node"
                )
                predecessors = await self.get_predecessors(length)
                predecessors = [ChordData(ip=p.ip, port=p.port) for p in predecessors]
                log.info(f"Predecessors of this node are => {predecessors}")
                return [ChordDataList(nodes=predecessors).to_json().encode()]

            case OperationCodes.NOTIFY.value, node_as_json_bytes:
                node = ChordNodeReference.from_json(node_as_json_bytes)
                log.info(f"The request is for notifying this node with {node}")
                await self.notify(node=node)
                log.info("We where notified")
                return [b"PONG"]

            case OperationCodes.PING.value,:
                log.info("The request is for pinging this node")
                return [b"PONG"]

            case OperationCodes.CLOSEST_PRECEDING_FINGER.value, id:
                id = int.from_bytes(id, byteorder="big")
                log.info(
                    f"The request is for finding the closest preceding finger of the id = {id}"
                )
                node = await self.closest_preceding_finger(id=id)
                if node:
                    log.info(f"The closest preceding finger of id={id} is {node}")
                    return node.to_multipart_message()
                log.warning("We get no closest preceding finger to give back")
                return None

            case _:
                log.warning("The request is not for any operation")

    # async def start_server(self):
    #     server_socket = self.zmq_context.socket(zmq.ROUTER)
    #     server_socket.bind(f"inproc://{CHORD_SERVICE_NAME}")

    #     backend_socket = self.zmq_context.socket(zmq.ROUTER)
    #     backend_socket.bind(f"inproc://{CHORD_BACKEND_NAME}")

    #     poller = Poller()
    #     poller.register(server_socket, zmq.POLLIN)
    #     poller.register(backend_socket, zmq.POLLIN)

    #     while True:
    #         socks = dict(await poller.poll())
    #         if server_socket in socks:
    #             pass
    #         if backend_socket in socks:
    #             pass
