from basic_imports import *
from chord_interface import ChordInterface

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

    def add(self, node: ChordInterface):
        if not isinstance(node, ChordInterface):
            raise TypeError("Only ChordInterface objects can be added to the list")

        self.list.append(node)
        self.list.sort(key=lambda x: (x.id - self.initial_id) % MAX_ID)

        if len(self.list) > self.capacity:
            self.list.pop()

    def get_successors(self, length: int):
        length = max(0, min(length, len(self.list) - 1))
        return self.list[:length]

    def get_successor(self):
        if len(self.list) == 0:
            return self.primary_node
        return self.list[0]

    def remove(self, node: ChordInterface):
        self.list = [n for n in self.list if n.id != node.id]

    def __str__(self):
        return f"SuccessorList({self.list})"

    def __repr__(self):
        return self.__str__()


class PredecessorList:
    def __init__(self, capacity: int, initial_id: int):
        self.capacity: int = capacity
        self.initial_id: int = initial_id
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

    def remove(self, node: ChordInterface):
        self.list = [n for n in self.list if n.id != node.id]

    def __str__(self):
        return f"PredecessorList({self.list})"

    def __repr__(self):
        return self.__str__()
