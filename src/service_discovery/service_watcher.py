from basic_imports import *
import zmq
from .service_announcement import ServiceAnnouncement
from .dns_registry import PeerRegistry, Peer
import datetime as dt
from loguru import logger
from .discovery_agent import DiscoveryAgent, JOINED, LEFT, KILL
import uuid
from threading import Thread


def create_pipe(ctx: zmq.Context) -> tuple[zmq.Socket, zmq.Socket]:
    """Create an inproc Pair pipe"""
    a, b = ctx.socket(zmq.PAIR), ctx.socket(zmq.PAIR)
    url = f"inproc://{uuid.uuid4().hex}"
    a.bind(url)
    b.connect(url)
    return a, b


class ServiceWatcher(object):
    ctx: zmq.Context
    """The zmq context"""
    pipe_to_agent: zmq.Socket
    """The pipe to communicate with the discovery agent"""

    def __init__(
        self,
        services_to_announce: list[ServiceAnnouncement] = [],
        expiration_time: dt.timedelta = dt.timedelta(seconds=3),
        # enable_logs: bool = True,
    ):
        self.logger = logger.bind(where="ServiceWatcher")
        """The logger for this class"""
        self.logger.bind(inside="__init__").info("Creating the Service Watcher")

        # self.ctx = parent_ctx
        self.ctx = zmq.Context()
        p0, p1 = create_pipe(self.ctx)
        self.pipe_to_agent = p0

        self.peer_registry: PeerRegistry = PeerRegistry()
        """The registry of peers"""

        self.agent = DiscoveryAgent(
            parent_ctx=self.ctx,
            pipe=p1,
            announcements=services_to_announce,
            expiration_time=expiration_time,
        )
        self.agent_thread = Thread(
            name="Agent Thread",
            target=self.agent.start,
            daemon=True,
        )
        self.agent_thread.start()
        killer_pipe, killed_pipe = create_pipe(self.ctx)
        self.killer_pipe = killer_pipe
        """Pipe to kill the updater"""
        self.updater = Thread(
            name="Service Updater Thread",
            target=lambda: self.update(self.pipe_to_agent, killed_pipe),
            daemon=True,
        )
        self.updater.start()

    def stop(self):
        """Stop the service watcher"""
        # TODO: This implementation is wrong
        self.agent.stop()
        self.pipe_to_agent.close()
        self.killer_pipe.send(KILL)
        # self.updater.
        self.ctx.term()

    def update(self, pipe_to_agent: zmq.Socket, service_pipe: zmq.Socket):
        """Update the service watcher"""
        # TODO: This implementation could be done with asyncio. But not important for now
        poller = zmq.Poller()
        poller.register(pipe_to_agent, zmq.POLLIN)
        poller.register(service_pipe, zmq.POLLIN)
        log = self.logger.bind(inside="update")
        while True:
            # self.logger.info("Checking events in the Updater")
            log.debug("Waiting for an event")
            # TODO: This is bad design. But is working
            events: list[tuple[zmq.SyncSocket, int]] = poller.poll()
            try:
                for socket, event in events:
                    if socket == pipe_to_agent and event == zmq.POLLIN:
                        message = socket.recv_multipart()
                        if message[0] == JOINED:
                            peer = Peer.from_json(message[1].decode("utf-8"))
                            self.peer_registry.add_peer(peer)
                            log.bind(peer=peer).debug(f"Peer joined")
                        elif message[0] == LEFT:
                            peer = Peer.from_json(message[1].decode("utf-8"))
                            self.peer_registry.remove_peer(peer)
                            log.bind(peer=peer).debug(f"Peer left")
                        else:
                            log.warning(f"Unknown message")
                    else:
                        # TODO: Improve this
                        log.warning(
                            f"Stopping the updater on event: {event} inside socket {socket}"
                        )
                        break
            except Exception as e:
                log.error(
                    f"An unexpected error has occurred inside the 'Updater' -> {e}"
                )

    def get_services(self, service_name: str, max_amount: int) -> list[Peer]:
        """Get the services from the registry"""
        return self.peer_registry.get_peers(service_name, max_amount)
