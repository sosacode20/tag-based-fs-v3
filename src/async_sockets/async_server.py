import socket
import asyncio
from loguru import logger
from typing import Callable, Any, Coroutine
from async_connection import AsyncConnection


class AsyncServer:
    def __init__(
        self,
        ip: str,
        port: int,
        handle_client_connection: Callable[[AsyncConnection], Coroutine[Any, Any, Any]],
        max_connections: int = 100,
    ):
        self.ip: str = ip
        """The ip address of this server"""
        self.port: int = port
        """The port in which the server is running"""
        self.max_connections: int = max_connections
        """The maximum number of connections that this server will allow"""
        # self.clients = []
        # """The current accepted connections"""
        self.logger = logger.bind(where="AsyncServer", address=f"{self.ip}:{self.port}")
        """The logger of this class"""
        self.handle_client_connection: Callable[
            [AsyncConnection], Coroutine[Any, Any, Any]
        ] = handle_client_connection
        """Callback for handling a connection"""
        self._is_closed: bool = False
        """Tells if the server is closed or no"""

    def is_closed(self):
        """Tells if the server is closed"""
        return self._is_closed

    async def run_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setblocking(False)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(self.max_connections)

        log = self.logger.bind(inside="run_server")
        log.info(f"Server started and listening at => {self.ip}:{self.port}")

        loop = asyncio.get_running_loop()

        semaphore = asyncio.Semaphore(self.max_connections)

        try:
            while True:
                async with semaphore:
                    log.info("Waiting for a new connection")
                    client_socket, addr = await loop.sock_accept(server_socket)
                    connection = AsyncConnection(
                        connection_socket=client_socket, address=addr
                    )
                    log.info(f"New connection with => {addr}")
                    asyncio.create_task(self.handle_client_connection(connection))
                    log.info("Connection handled")
        except KeyboardInterrupt:
            log.error("The server is stopping because of the user => CTRL + C")
            # server_socket.settimeout(0.5)
            # server_socket.close()
            # log.error("The server socket has been closed")
        except asyncio.CancelledError as canceled:
            log.error(
                f"The server is stopping because of an asyncio canceled exception => {canceled}"
            )
            # server_socket.settimeout(0.5)
            # server_socket.close()
            # log.error("The server has been closed")
        except Exception as e:
            log.error(
                f"The server is stopping because of an unhandled exception => {e}"
            )
            # server_socket.settimeout(0.5)
            # server_socket.close()
            # log.error("The server has been closed")
        finally:
            server_socket.settimeout(0.5)
            server_socket.close()
            self._is_closed = True
            log.info("Server closed")
            return
