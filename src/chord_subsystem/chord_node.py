from .chord_interface import (
    ChordInterface,
    ChordFailureTypes,
    ChordFailure,
    ChordSuccess,
    in_between,
)
from .chord_reference import ChordNodeReference
from .successor_list import SuccessorList, PredecessorList
from loguru import logger
import asyncio


class ChordNode(ChordInterface):
    def __init__(self, ip: str, port: int, m: int = 160):
        super().__init__(ip, port)
        self.ref: ChordInterface = ChordNodeReference(self.ip, self.port)
        """The Chord reference to this node. Use this to make calls to yourself"""
        self.m: int = m
        """The number of bits in the hash/key space"""
        self.finger: list[ChordInterface] = [self.ref] * self.m
        """Finger Table of this `ChordNode`. It includes the actual node as the first reference"""
        self.successors: SuccessorList = SuccessorList(capacity=3, initial_id=self.ref)
        """The successor list"""
        self.predecessors: PredecessorList = PredecessorList(
            capacity=3, initial_id=self.ref
        )
        """The predecessor list"""
        self.logger = logger.bind(where="ChordNode", this_node=self.ref)
        """The logger of this class"""

    def start(self):
        """Start the node. This function MUST be called from a new thread"""
        asyncio.run(self.run())

    async def run(self):
        """This is the function that creates all the coroutines to run the node"""

        await asyncio.gather(
            self.start_server(),
            self.stabilize(),
            self.fix_fingers(),
            self.check_predecessor(),
        )

    # region Chord RPC methods

    async def get_successors(self, length: int):
        return ChordSuccess(self.successors.get_successors(length=length))

    async def get_predecessors(self, length: int):
        return ChordSuccess(result=self.predecessors.get_predecessors(length=length))

    async def find_successor(self, id: int):
        node = await self.find_predecessor(id)
        match node:
            case ChordSuccess():
                res = node.result
                # return res.get

    async def find_predecessor(self, id: int):
        return await super().find_predecessor(id)

    async def closest_preceding_finger(self, id: int):
        return await super().closest_preceding_finger(id)

    async def notify(self, node: ChordInterface):
        return await super().notify(node)

    async def ping(self):
        return await super().ping()

    # endregion

    # region Chord Parallel methods

    async def join(self, node: ChordNodeReference):
        """Join the ring with the node"""
        # TODO: Implement this
        pass

    async def stabilize(self):
        """Maintain the successor list"""
        # TODO: Implement this
        pass

    async def fix_fingers(self):
        """Periodically refresh the finger table"""
        # TODO: Implement this
        pass

    async def check_predecessor(self):
        """Check if the predecessor is alive"""
        # TODO: Implement this
        pass

    # endregion
