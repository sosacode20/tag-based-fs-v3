from basic_imports import *
from enum import IntFlag
from typing import Self
import math
from loguru import logger

from rich import print, print_json


class FrameFlags(IntFlag):
    MORE_FRAMES = 0b00000001
    """A flag indicating that there are more frames to come"""
    LONG_FRAME = 0b00000010
    """A flag that indicates that the frame size is encoded as a
    64-bit unsigned integer in network byte order. If the flag is not present,
    the frame size is encoded as a single byte in network byte order.
    """


# EXAMPLE:
# flags = FrameFlags.MORE_FRAMES | FrameFlags.LONG_FRAME


class Frame:
    """A frame of data to send over the network"""

    def __init__(self, data: bytes, more_frames: bool = False):
        flags: FrameFlags = FrameFlags.MORE_FRAMES if more_frames else 0
        self.data_body: bytes = data
        """The bytes to send in the frame"""
        self.size: bytes = 0b00000000
        """The size of the frame"""
        size_field = math.ceil(
            math.log2(len(self.data_body))
        )  # The byte size of the size field
        if size_field <= 8:  # If the size field can be represented in a single byte
            self.size: bytes = len(self.data_body).to_bytes(length=1, byteorder="big")
        else:
            self.size: bytes = len(self.data_body).to_bytes(length=8, byteorder="big")
            flags |= FrameFlags.LONG_FRAME
        self.flags: bytes = flags.to_bytes(length=1, byteorder="big")
        """The flags to send in the frame"""

    def to_bytes(self) -> bytes:
        """Transform the frame to bytes"""
        return self.flags + self.size + self.data_body

    def __str__(self):
        return f"Frame(flags={self.flags}, size={self.size}, data={self.data_body})"

    def __repr__(self):
        return str(self)


def frames_from_multipart_message(message: list[bytes]) -> list[Frame]:
    """Return a list of frames from the given bytes"""
    frames: list[Frame] = []
    for i, data in enumerate(message):
        frames.append(Frame(data, more_frames=i < len(message) - 1))
    return frames


def frames_to_bytes(frames: list[Frame]) -> bytes:
    """Return the bytes representation of the given frames"""
    return b"".join(frame.to_bytes() for frame in frames)


# EXAMPLE:

# message = [b"Hello", b"World!!"]

# frames = frames_from_multipart_message(message)
# print(f"The frames are => {str(frames)}")

# byte_message = frames_to_bytes(frames)
# print(f"The byte message is => {byte_message}")
