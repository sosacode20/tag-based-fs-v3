from basic_imports import *
from .chord_interface import ChordInterface, in_between
import asyncio
from typing import Optional
from loguru import logger

MAX_ID = 2**160


class SuccessorList:
    def __init__(
        self,
        capacity: int,
        primary_node: ChordInterface,
    ):
        self.capacity: int = capacity
        """The maximum number of successors that the list can have."""
        self.primary_node: ChordInterface = primary_node
        """The node that has this successors list."""
        self.initial_id: int = primary_node.id
        """The id of the primary node."""
        self.list: list[ChordInterface] = []
        """The list of successors."""
        self.logger = logger.bind(where="SuccessorList", primary_node=primary_node)
        """The logger for this class"""

    def add(self, *nodes: ChordInterface):
        """
        Add a list of nodes to the successor list."""
        log = self.logger.bind(inside="add", nodes=nodes)
        log.info(f"Adding the nodes {nodes} to the successor list => {self.list}")
        if not all(isinstance(n, ChordInterface) for n in nodes):
            log.exception("Only ChordInterface objects can be added to the list")
            raise TypeError("Only ChordInterface objects can be added to the list")
        self.list.extend(nodes)
        self._sort_successors()

    def _sort_successors(self):
        """
        Sort the list of successors by the distance to the primary node.

        Also, this method maintains the size of the successor list
        """
        log = self.logger.bind(inside="_sort_successors", list=self.list)
        log.debug(f"Before sorting the list of successors => {self.list}")

        self.remove(self.primary_node)
        self.list = list(set(self.list))  # Remove duplicates

        self.list.sort(key=lambda x: (x.id - self.initial_id) % MAX_ID)
        if len(self.list) > self.capacity:
            log.bind(actual_length=len(self.list)).debug(
                f"The list has more than {self.capacity} elements => {self.list}"
            )
            self.list = self.list[: self.capacity]
        log.bind(list=self.list).debug(
            f"After sorting the list of successors => {self.list}"
        )

    async def get_successors(self, length: int) -> list[ChordInterface]:
        """
        Returns the first `length` successors of the node.
        """
        log = self.logger.bind(inside="get_successors", length=length)
        log.info(f"Obtaining the first {length} successors of the node => {self.list}")
        if len(self.list) == 0:
            log.debug("The list is empty, returning an empty list")
            return []

        length = max(0, min(length, len(self.list) - 1))
        res = self.list[:length]
        log.bind(list=res).debug(f"Returning the first {length} successors => {res}")
        return self.list[:length]

    async def get_successor(self) -> ChordInterface:
        """
        Returns the first successor of the node.

        If the list is empty, it returns the primary node
        (The node that has this successors list).
        """
        log = self.logger.bind(inside="get_successor")
        log.info(f"Obtaining the first successor of this node {self.primary_node}")
        if len(self.list) == 0:
            log.debug(
                f"The list is empty, returning the primary node => {self.primary_node}"
            )
            return self.primary_node
        log.bind(successor=self.list[0]).debug(
            f"Returning the first successor => {self.list[0]}"
        )
        return self.list[0]

    async def make_node_ping(self, node: ChordInterface) -> tuple[bool, ChordInterface]:
        """
        Pings a node and returns a tuple with the result of the ping
        """
        # return (await node.ping(), node)
        return (await node.notify(self.primary_node), node)

    async def get_pred_and_successors(
        self, node: ChordInterface
    ) -> Optional[tuple[ChordInterface, list[ChordInterface]]]:
        """
        Returns the predecessor and the successors of a node.
        """
        # parallel = [node.predecessor, node.get_successors(self.capacity)]
        # Make both queries in parallel with asyncio and wait for a maximum
        # of 3 seconds. Both queries must have a result by the end time
        # if some query has not finish by the end. Then return None
        log = self.logger.bind(inside="get_pred_and_successors", node=node)
        log.info(f"Getting the predecessor and successors nodes of => {node}")

        log.debug(
            f"Creating the tasks for obtaining the predecessor and successors nodes"
        )
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
            log.debug(f"Tasks incomplete. Node {node} MAY be dead")
            return None
        pred: Optional[ChordInterface] = None
        successors: Optional[list[ChordInterface]] = None
        for t in done:
            t_name = t.get_name()
            if t_name == "predecessor":
                pred = t.result()
                log.bind(pred=pred).debug(
                    f"We got a result for the predecessor => {pred}"
                )
            elif t_name == "successors":
                successors = t.result()
                log.bind(successors=successors).debug(
                    f"We got a result for the successors of the node => {successors}"
                )
        if pred is None or successors is None:
            log.debug("We could not get a predecessor of successors list from the node")
            return None
        log.info(
            f"Predecessor {pred} and Successors ({successors}) found from the node"
        )
        return pred, successors

    async def get_first_alive_successor(
        self, timeout: float = 3
    ) -> Optional[ChordInterface]:
        """
        Returns the first successor node that is alive.
        It also updates the list of successors removing all
        dead peers.
        """
        log = self.logger.bind(
            inside="get_first_alive_successor",
            timeout=timeout,
            initial_successors=self.list,
        )
        log.info("Getting the first alive successor")
        if len(self.list) == 0:
            log.info("The successor list is empty")
            return None
        log.debug("Doing a PING to every successor node in our list")
        pings = [self.make_node_ping(node) for node in self.list]
        done, _ = await asyncio.wait(
            (asyncio.create_task(p) for p in pings), timeout=timeout
        )
        alive_nodes: list[ChordInterface] = []
        for task in done:
            res, node = task.result()
            if res:
                log.debug(f"Node {node} is alive")
                alive_nodes.append(node)
        if len(alive_nodes) == 0:
            log.info("All successor peers are dead")
            return None
        self.list = alive_nodes
        self._sort_successors()
        log.bind(successors=self.list, first_succ=self.list[0]).info(
            "The successor list now has only alive nodes and we can return the first one"
        )
        return self.list[0]

    async def update(self):
        """
        This method stabilize the successors list
        """
        log = self.logger.bind(inside="update", initial_successors=self.list)
        log.info(
            f"Updating the successor list of this node {self.primary_node}. STABILIZING..."
        )
        first_node = await self.get_first_alive_successor()
        if not first_node:
            log.info("There is no alive node in the successor list")
            return
        log.debug(f"The first alive node is {first_node}")
        log.debug(f"Asking for the predecessor and successors of node {first_node}")
        res = await self.get_pred_and_successors(first_node)
        if not res:
            log.info(
                "The first alive node give us no results for predecessor and successors list"
            )
            return
        predecessor, successors = res
        log.bind(pred=predecessor, successors=successors).debug(
            f"We got the predecessor and successors of node {first_node}"
        )
        log.debug(f"The predecessor is => {predecessor}")
        log.debug(f"The successors of node {first_node} are => {successors}")

        # self.list = [predecessor] + successors
        self.list = [first_node] + successors
        self._sort_successors()
        log.bind(updated_successors=self.list).debug(
            f"We have a new updated successor list => {self.list}"
        )

        if in_between(predecessor.id, self.initial_id, first_node.id):
            log = log.bind(pred=predecessor, actual_successor=first_node)
            log.debug(f"The predecessor is actually a better successor for us")
            log.debug("Asking for the successors of this predecessor")
            new_successors = await predecessor.get_successors(self.capacity)

            match new_successors:
                case None:
                    log.debug(
                        "The predecessor don't give an answer. So, the successor list remains the same"
                    )
                case []:
                    log.debug(
                        "The predecessor has no successors. But we add it as the first element of the list, because it is a better successor"
                    )
                    self.list = [predecessor] + self.list
                    await predecessor.notify(self.primary_node)
                    self._sort_successors()
                case pred_successors:
                    log.debug(
                        f"The predecessor has successors => {pred_successors}. We update the successor list"
                    )
                    self.list = [predecessor] + pred_successors + self.list
                    await predecessor.notify(self.primary_node)
                    self._sort_successors()

        log.info(
            f"Exiting the Stabilize method of this node {self.primary_node} in the successor list"
        )
        # self.list = [predecessor] + successors

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
        """The maximum number of predecessors that the list can have."""
        self.primary_node: ChordInterface = primary_node
        """The node that has this predecessors list."""
        self.initial_id: int = primary_node.id
        """The id of the primary node."""
        self.list: list[ChordInterface] = []
        """The list of predecessors."""
        self.logger = logger.bind(where="PredecessorList", primary_node=primary_node)
        """The logger for this class"""

    def add(self, *nodes: ChordInterface):
        """
        Add a list of nodes to the successor list."""
        if not all(isinstance(n, ChordInterface) for n in nodes):
            raise TypeError("Only ChordInterface objects can be added to the list")
        self.list.extend(nodes)
        self._sort_predecessors()

    async def get_predecessors(self, length: int):
        log = self.logger.bind(inside="get_predecessors", length=length)
        log.info(f"Obtaining the first {length} successors of the node => {self.list}")
        if len(self.list) == 0:
            log.debug("The list is empty, returning an empty list")
            return []

        length = max(0, min(length, len(self.list) - 1))
        res = self.list[:length]
        log.bind(list=res).debug(f"Returning the first {length} predecessors => {res}")
        return self.list[:length]

    async def get_predecessor(self):
        log = self.logger.bind(inside="get_predecessor")
        log.info(f"Obtaining the first predecessor of this node {self.primary_node}")
        if len(self.list) == 0:
            log.debug(
                f"The list is empty, returning the primary node as predecessor => {self.primary_node}"
            )
            return self.primary_node
        log.bind(successor=self.list[0]).debug(
            f"Returning the first predecessor => {self.list[0]}"
        )
        return self.list[0]

    def remove(self, node: ChordInterface):
        self.list = [n for n in self.list if n.id != node.id]

    def _sort_predecessors(self):
        """
        Sort the list of predecessors by the distance to the primary node.

        Also, this method maintains the size of the successor list
        """
        log = self.logger.bind(inside="_sort_predecessors", list=self.list)
        log.debug(f"Before sorting the list of predecessors => {self.list}")

        self.remove(self.primary_node)
        self.list = list(set(self.list))  # Remove duplicates

        self.list.sort(key=lambda x: (self.initial_id - x.id) % MAX_ID)

        if len(self.list) > self.capacity:
            log.bind(actual_length=len(self.list)).debug(
                f"The list has more than {self.capacity} elements => {self.list}"
            )
            self.list = self.list[: self.capacity]
        log.bind(list=self.list).debug(
            f"After sorting the list of successors => {self.list}"
        )

    async def make_node_ping(self, node: ChordInterface) -> tuple[bool, ChordInterface]:
        """
        Pings a node and returns a tuple with the result of the ping
        """
        return (await node.ping(), node)

    async def update(self):
        """
        This method stabilize the predecessors list
        """
        log = self.logger.bind(inside="update", initial_predecessors=self.list)
        log.info(
            f"Updating the predecessor list of this node {self.primary_node}. STABILIZING..."
        )
        if len(self.list) == 0:
            log.info("The predecessor list is empty")
            return
        log.debug("Doing a PING to every predecessor node in our list")
        pings = [self.make_node_ping(node) for node in self.list]
        done, _ = await asyncio.wait(
            (asyncio.create_task(p) for p in pings),
            timeout=3,
        )
        alive_nodes: list[ChordInterface] = []
        for task in done:
            res, node = task.result()
            if res:
                log.debug(f"Node {node} is alive")
                alive_nodes.append(node)
        if len(alive_nodes) == 0:
            log.info("All predecessor peers are dead")
            return None

        self.list = alive_nodes
        self._sort_predecessors()
        log.bind(updated_predecessors=self.list).debug(
            f"We have a new updated predecessor list => {self.list}"
        )

    def __len__(self):
        return len(self.list)

    def __str__(self):
        return f"PredecessorList({self.list})"

    def __repr__(self):
        return self.__str__()
