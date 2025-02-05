from basic_imports import *
import socket
import asyncio
from rich import print
from .frame_utils import (
    Frame,
    FrameFlags,
    frames_from_multipart_message,
    frames_to_bytes,
)
from loguru import logger
from typing import Any
import math
from pathlib import Path
from file_helpers.file_helper import FileHelper
from file_helpers.file_download_helper import FileDownloadHelper
from enum import Enum, unique
from abc import ABC


def closest_pow_of_2(n: int) -> int:
    """Calculates the closest power of 2 to the given number"""
    return 2 ** int(math.log2(n))


class AsyncSocketException(Exception):
    pass


class AsyncSocketTimeout(AsyncSocketException):
    pass


class AsyncSocketIncompleteRead(AsyncSocketException):
    pass


class AsyncSocketDisconnected(AsyncSocketException):
    pass


@unique
class FileOperations(Enum):
    COMPLETED = b"COMPLETED"
    """Instruction for telling that we don't want more chunks of the file"""
    FETCH = b"FETCH"
    """Instruction for asking for a chunk"""
    UPLOAD_CHUNK = b"UPLOAD_CHUNK"
    """Instruction to upload a chunk"""
    ERROR = b"ERROR"
    """Instruction for telling that an error has occurred"""
    INVALID_CHUNK = b"INVALID_CHUNK"
    """Tells that the request for the chunk was invalid"""
    CANCELLED = b"CANCELLED"
    """Tell the connection peer you want to cancel the operation"""


@unique
class FailureOptions(Enum):
    """Options for the failure of the file transfer"""

    UNKNOWN = b"UNKNOWN"
    """Unknown error"""
    TIMEOUT = b"TIMEOUT"
    """Connection timeout"""
    DISCONNECTED = b"DISCONNECTED"
    """The peer is disconnected"""
    CANCELLED = b"CANCELLED"
    """Cancelled error"""
    CORRUPT_FILE = b"CORRUPT_FILE"
    """The downloaded file is corrupted"""
    BAD_REQUEST = b"BAD_REQUEST"
    """The peer says we make a bad request"""


class AsyncConnection:
    """This class represents an async connection with another peer"""

    def __init__(
        self,
        connection_socket: socket.socket,
        address: Any,
        buffer_max_size: int = 1024**2,  # 1MB
    ):
        self.logger = logger.bind(where="AsyncConnection")
        """The logger for this class"""
        self.connection_socket: socket.socket = connection_socket
        """The socket to which the communication is happening"""
        self.address: Any = address
        """The address of the connection"""
        self.buffer_max_size: int = buffer_max_size
        """The maximum size for the buffer"""
        self.buffer: bytes = b""
        """The buffer"""
        self.loop = asyncio.get_running_loop()
        """The async loop"""
        # self.loop.create_task()

    def __str__(self):
        return f"AsyncConnection({self.address})"

    def __repr__(self):
        return str(self)

    async def send_multipart(self, message: list[bytes]):
        """Send a multipart message to the connection"""
        loop = asyncio.get_running_loop()
        frames = frames_from_multipart_message(message)
        log = self.logger.bind(inside="send_multipart", frames=frames)
        log.info(f"Sending a multipart message => with {len(frames)} frames")
        try:
            for i, frame in enumerate(frames):
                log.debug(f"Sending frame number {i}")
                # self.writer.write(frame.to_bytes())
                # await self.writer.drain()
                await loop.sock_sendall(self.connection_socket, frame.to_bytes())
                # await loop.sock_sendall()
                log.debug(f"Frame {i} successfully delivered")
        except Exception as e:
            log.error(f"The socket was unable to deliver the message => {e}")
            raise
        log.info(f"Multipart message was sended successfully")

    def close_connection(self):
        """This function close the connection"""
        log = self.logger.bind(inside="close_connection")
        log.info("Closing the connection")
        if self.connection_socket:
            try:
                self.connection_socket.settimeout(0.5)
                self.connection_socket.close()
            except BaseException as e:
                log.exception(
                    f"An error occurred when closing the connection with socket"
                )
            finally:
                self.address = None
                self.connection_socket = None
        log.info("Connection closed")

    async def _fill_recv_buffer(self):
        """Populate the receiving buffer"""
        try:
            loop = asyncio.get_running_loop()
            to_fill = closest_pow_of_2(self.buffer_max_size - len(self.buffer))
            data = await asyncio.wait_for(
                loop.sock_recv(self.connection_socket, to_fill), timeout=2
            )
        except Exception:
            pass
        else:
            if data:
                self.buffer += data
            else:
                raise AsyncSocketDisconnected()

    async def _recv(self, n_bytes: int) -> bytes:
        """This routine continues until it receives all the n_bytes"""
        data: bytes = b""
        try:
            while len(data) < n_bytes:
                diff = n_bytes - len(data)
                data += self.buffer[:diff]

                self.buffer = self.buffer[diff:]
                if len(data) < n_bytes:
                    await self._fill_recv_buffer()
            return data
        except AsyncSocketDisconnected:
            self.logger.error(f"The socket is disconnected")
            raise
        except Exception as e:
            raise

    async def recv_multipart(self) -> list[bytes]:
        """Receive a multipart message from the connection"""
        frames = []
        log = self.logger.bind(inside="recv_multipart")
        log.info("Starting the routine of receiving a multipart message")
        # loop = asyncio.get_running_loop()

        try:
            while True:
                log.debug("Waiting for a frame")
                # flags = await self.reader.readexactly(1)
                # flags = await loop.sock_recv(self.connection_socket, 1)
                flags = await self._recv(1)
                flags = FrameFlags(int.from_bytes(flags))
                log.bind(frame_flags=flags).debug(
                    "Received a flags field from a new frame"
                )
                if flags & FrameFlags.LONG_FRAME:
                    # size = await loop.sock_recv(self.connection_socket, 8)
                    size = await self._recv(8)
                    size = int.from_bytes(size, byteorder="big")
                else:
                    # size = await self.reader.readexactly(1)
                    size = await self._recv(1)
                    size = int.from_bytes(size, byteorder="big")
                log.debug(
                    f"The number of bytes of data in the frame is => {size} bytes"
                )
                # data = await self.reader.readexactly(size)
                data = await self._recv(size)
                log.bind(frame_body=data).debug(f"Received a new message")

                more_frames = flags & FrameFlags.MORE_FRAMES
                new_frame: Frame = Frame(data, more_frames=more_frames)
                frames.append(new_frame)
                log.bind(frame=new_frame).info(f"Frame received successfully")
                if not more_frames:
                    break
        except AsyncSocketDisconnected:
            raise
        except Exception as e:
            log.error(f"An unknown error has been received => {e}")
            raise
        return [frame.data_body for frame in frames]

    async def send_file(
        self,
        file_helper: FileHelper,
        timeout: float = 20,
    ) -> tuple[bool, FailureOptions]:
        """Sends a file to the peer. BUT the peer MUST be calling the function `recv_file` in the moment this method is called"""
        log = self.logger.bind(inside="send_file", file_path=file_helper.file_path)

        log.info(f"Sending file {file_helper.file_path}")
        try:
            while True:
                log.info("Waiting for a fetch or complete instruction")
                request = await asyncio.wait_for(self.recv_multipart(), timeout=timeout)
                # instruction, *rest = request
                log.info("Request received")

                match request:
                    case FileOperations.COMPLETED.value,:
                        log.info("File upload was completed")
                        return True, FailureOptions.UNKNOWN
                    case FileOperations.ERROR.value, reason:
                        try:
                            reason = reason.decode()
                            log.bind(error_reason=reason).error(
                                f"File Upload failed, reason => {reason}"
                            )
                        except:
                            log.error(
                                "There was an error in the decoding of the reason for the upload failure. It is not in UTF-8 format"
                            )
                        finally:
                            return False, FailureOptions.BAD_REQUEST
                    case FileOperations.FETCH.value, offset, size:
                        offset = int.from_bytes(offset)
                        size = int.from_bytes(size)

                        if not file_helper.valid_chunk_request(
                            offset=offset, chunk_size=size
                        ):
                            await asyncio.wait_for(
                                self.send_multipart(
                                    [FileOperations.INVALID_CHUNK.value]
                                ),
                                timeout=timeout,
                            )
                            return False, FailureOptions.BAD_REQUEST

                        chunk = await file_helper.get_chunk_async(offset, size)
                        await asyncio.wait_for(
                            self.send_multipart(
                                [FileOperations.UPLOAD_CHUNK.value, chunk]
                            ),
                            timeout=timeout,
                        )
                    case _:
                        log.warning(
                            f"The connection peer give us an unknown instruction"
                        )
                        log.warning(
                            f"Sending an {FileOperations.ERROR.value} message to the peer"
                        )
                        await asyncio.wait_for(
                            self.send_multipart(
                                [FileOperations.ERROR.value],
                                b"Unknown instruction request received. Closing our connection",
                            ),
                            timeout=timeout,
                        )
                        log.warning(
                            f"Stopping the file upload because of this unknown instruction"
                        )
                        return False, FailureOptions.BAD_REQUEST
        except asyncio.TimeoutError:
            log.error(
                "Stopping the operation of sending the file because the operation reached the timeout"
            )
            return False, FailureOptions.TIMEOUT
        except asyncio.CancelledError:
            log.info("The user wants to cancel the upload of the file.")
            log.debug("Sending a Cancel request to the connection peer")
            await asyncio.wait_for(
                self.send_multipart([FileOperations.CANCELLED.value]), timeout=timeout
            )
            log.debug("Peer received the message")
            raise
        except AsyncSocketDisconnected:
            log.error(
                "Failure in the operation to send the file, because the peer is disconnected"
            )
            return False, FailureOptions.DISCONNECTED

    async def recv_file(
        self,
        file_download_helper: FileDownloadHelper,
        max_chunk_size: int = 1 * 1024**2,
        timeout: float = 20,
    ) -> tuple[bool, FailureOptions]:
        """
        This method receives a file and store it's chunks with the help of the `FileDownloadHelper`
        given as parameter (IT NEEDS TO BE INITIALIZED) before calling this method.
        ALSO, the peer MUST have called the method `send_file` when we call this method.
        """
        log = self.logger.bind(
            inside="recv_file", file_name=file_download_helper.file_name
        )
        log.info(f"Starting the download of the file {file_download_helper.file_name}")

        meta = await file_download_helper.read_metadata()
        bytes_to_download = meta.file_size - meta.downloaded_bytes
        downloaded_bytes = meta.downloaded_bytes
        while bytes_to_download - downloaded_bytes:
            to_download = min(max_chunk_size, bytes_to_download - downloaded_bytes)
            log.info(
                f"Asking the client to upload a chunk with offset {downloaded_bytes} and size {to_download} bytes"
            )
            await asyncio.wait_for(
                self.send_multipart(
                    [
                        FileOperations.FETCH.value,
                        downloaded_bytes.to_bytes(8),
                        to_download.to_bytes(4),
                    ]
                ),
                timeout=timeout,
            )
            log.info("Fetch request sended")
            log.info("Waiting for the chunk...")

            response = await asyncio.wait_for(self.recv_multipart(), timeout=timeout)

            log.info(f"Client response received")

            match response:
                case FileOperations.CANCELLED.CANCELLED,:
                    log.info("Connection peer wants to cancel the upload of the file")
                    return False, FailureOptions.CANCELLED
                case FileOperations.INVALID_CHUNK.value,:
                    log.error(
                        "The peer connection says that we make a wrong chunk request"
                    )
                    return False, FailureOptions.BAD_REQUEST
                case FileOperations.UPLOAD_CHUNK.value, chunk:
                    chunk_size = len(chunk)
                    assert chunk_size > 0, "The received chunk was empty"
                    log.info(f"Received a chunk with size => {chunk_size}")
                    log.debug("Writing chunk in our storage")
                    await file_download_helper.write_chunk(chunk=chunk)
                    log.debug("Writing in storage was successful")
                    downloaded_bytes += chunk_size
                case FileOperations.ERROR.value, reason:
                    log.bind(error_reason=reason).error(
                        f"The peer connection says that there was an error"
                    )
                    log.warning("Stopping the routine of receiving the file")
                    return False, FailureOptions.BAD_REQUEST
                case _:
                    log.error("Received an unknown request")
                    log.info(
                        "Sending a message to the peer that we detect an unknown request from them"
                    )
                    await asyncio.wait_for(
                        self.send_multipart([FileOperations.ERROR.value]),
                        timeout=timeout,
                    )
                    log.info("Error message delivered successfully")
                    return False, FailureOptions.BAD_REQUEST

        log.info("All bytes from file where received")
        log.info("Sending a message of download completed")
        await self.send_multipart([FileOperations.COMPLETED.value])
        log.info("Confirmation sended")
        return True, FailureOptions.UNKNOWN
