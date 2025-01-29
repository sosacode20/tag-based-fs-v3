from async_sockets.async_connection import AsyncConnection
from loguru import logger
from loguru._logger import Logger
import socket
import asyncio


async def create_client(to_ip: str, to_port: int, my_logger: Logger) -> AsyncConnection:
    """Create an AsyncConnection that serves as a client connecting to a server"""
    log = my_logger.bind(inside="create_client", to_ip=to_ip, to_port=to_port)
    log.info("Creating the client")
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setblocking(False)
    address = (to_ip, to_port)
    log.info("Connecting to server")
    # client_socket.connect(address)
    loop = asyncio.get_running_loop()
    await loop.sock_connect(client_socket, address=address)
    log.info("Connection Successful")
    return AsyncConnection(connection_socket=client_socket, address=address)
