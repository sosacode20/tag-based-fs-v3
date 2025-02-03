from basic_imports import *
from .chord_interface import ChordInterface
import asyncio
from typing import Optional

MAX_ID = 2**160


class SuccessorList:
    def __init__(
        self,
        capacity: int,
        primary_node: ChordInterface,
    ):
        self.capacity: int = capacity
        self.primary_node: ChordInterface = primary_node
        self.initial_id: int = primary_node.id
        self.list: list[ChordInterface] = []

    def add(self, *nodes: ChordInterface):
        """
        Add a list of nodes to the successor list."""
        if not all(isinstance(n, ChordInterface) for n in nodes):
            raise TypeError("Only ChordInterface objects can be added to the list")
        self.list.extend(nodes)
        self.list.sort(key=lambda x: (x.id - self.initial_id) % MAX_ID)
        if len(self.list) > self.capacity:
            self.list = self.list[: self.capacity]

    async def get_successors(self, length: int):
        """
        Returns the first `length` successors of the node.
        """
        if len(self.list) == 0:
            return []
        length = max(0, min(length, len(self.list) - 1))
        return self.list[:length]

    async def get_successor(self) -> ChordInterface:
        """
        Returns the first successor of the node.

        If the list is empty, it returns the primary node
        (The node that has this successors list).
        """
        if len(self.list) == 0:
            return self.primary_node
        return self.list[0]

    async def make_node_ping(self, node: ChordInterface) -> tuple[bool, ChordInterface]:
        """
        Pings a node and returns a tuple with the result of the ping
        """
        return (await node.ping(), node)

    async def get_pred_and_successors(
        self, node: ChordInterface
    ) -> Optional[tuple[ChordInterface, list[ChordInterface]]]:
        # parallel = [node.predecessor, node.get_successors(self.capacity)]
        # Make both queries in parallel with asyncio and wait for a maximum
        # of 3 seconds. Both queries must have a result by the end time
        # if some query has not finish by the end. Then return None
        done, _ = await asyncio.wait(
            [
                asyncio.create_task(node.predecessor, name="predecessor"),
                asyncio.create_task(
                    node.get_successors(self.capacity), name="successors"
                ),
            ],
            return_when=asyncio.ALL_COMPLETED,
            timeout=3,
        )
        if len(done) != 2:
            return None
        pred: Optional[ChordInterface] = None
        successors: Optional[list[ChordInterface]] = None
        for t in done:
            t_name = t.get_name()
            if t_name == "predecessor":
                pred = t.result()
            elif t_name == "successors":
                successors = t.result()
        if pred is None or successors is None:
            return None
        return pred, successors

    async def get_first_alive_successor(
        self, timeout: float = 3
    ) -> Optional[ChordInterface]:
        """
        Returns the first successor node that is alive.
        It also updates the list of successors removing all
        dead peers.
        """
        if len(self.list) == 0:
            return None
        pings = [self.make_node_ping(node) for node in self.list]
        done, _ = await asyncio.wait(
            (asyncio.create_task(p) for p in pings), timeout=timeout
        )
        alive_nodes: list[ChordInterface] = []
        for task in done:
            res, node = task.result()
            if res:
                alive_nodes.append(node)
        if len(alive_nodes) == 0:
            return None
        alive_nodes.sort(key=lambda x: (x.id - self.initial_id) % MAX_ID)
        self.list = alive_nodes
        return alive_nodes[0]

    async def update(self):
        """
        This method stabilize the successors list
        """
        first_node = await self.get_first_alive_successor()
        if not first_node:
            return
        predecessor = await first_node.predecessor

    def remove(self, node: ChordInterface):
        """
        Removes a node from the successor list.
        """
        self.list = [n for n in self.list if n.id != node.id]

    def __len__(self):
        return len(self.list)

    def __str__(self):
        return f"SuccessorList({self.list})"

    def __repr__(self):
        return self.__str__()


class PredecessorList:
    def __init__(
        self,
        capacity: int,
        primary_node: ChordInterface,
    ):
        self.capacity: int = capacity
        self.primary_node: ChordInterface = primary_node
        self.initial_id: int = primary_node.id
        self.list: list[ChordInterface] = []

    def add(self, node: ChordInterface):
        if not isinstance(node, ChordInterface):
            raise TypeError("Only ChordInterface objects can be added to the list")

        self.list.append(node)
        self.list.sort(key=lambda x: (self.initial_id - x.id) % MAX_ID, reverse=True)

        if len(self.list) > self.capacity:
            self.list.pop()

    def get_predecessors(self, length: int):
        length = max(0, min(length, len(self.list) - 1))
        return self.list[:length]

    def get_predecessor(self):
        if len(self.list) == 0:
            return self.primary_node
        return self.list[0]

    def remove(self, node: ChordInterface):
        self.list = [n for n in self.list if n.id != node.id]

    def __len__(self):
        return len(self.list)

    def __str__(self):
        return f"PredecessorList({self.list})"

    def __repr__(self):
        return self.__str__()
