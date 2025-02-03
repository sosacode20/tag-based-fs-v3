from basic_imports import *
from pydantic import BaseModel, Field, UUID4
from .service_announcement import ServiceAnnouncement
from datetime import datetime, timezone
from typing import *
import uuid
from loguru import logger


class Peer(BaseModel):
    """This class is used to store the information of a peer"""

    uuid: UUID4 = Field(description="The UUID of the peer", default=uuid.uuid4())
    """The UUID of the peer"""

    service: ServiceAnnouncement = Field(
        description="The service information of the peer"
    )
    """The service configuration"""
    expiration_time: datetime = Field(
        description="The UTC time when the peer expires",
        default_factory=lambda: datetime.now(timezone.utc),
        exclude=True,
    )
    """The UTC time when the peer expires"""

    def __eq__(self, value):
        if isinstance(value, Peer):
            return self.uuid == value.uuid
        return False

    def is_expired(self) -> bool:
        """Check if the peer has expired"""
        # Get the current time and compare it with the expiration time in UTC
        return datetime.now(timezone.utc) > self.expiration_time

    def update_expiration_time(self, expiration_time: datetime) -> None:
        """Update the expiration time of the peer"""
        self.expiration_time = expiration_time.astimezone(timezone.utc)

    def to_json(self):
        """Convert the object to a JSON string"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str):
        """Create an instance of the class from a JSON string"""
        res = uuid.uuid4().hex.encode("utf-8")
        return cls.model_validate_json(json_str)

    # @model_validator(mode="after")
    # def pos_init(self) -> Self:
    #     self.expiration_time = self.expiration_time.astimezone(timezone.utc)
    #     return self


class PeerRegistry(BaseModel):
    """This class is used to store the peers"""

    services_and_peers: dict[str, list[Peer]] = Field(
        description="The dictionary of peers per service",
        default={},
    )
    """The dictionary of services and the peers that offered it"""
    max_peers_per_service: int = Field(
        gt=0,
        lt=1000,
        description="The maximum number of peers per service",
        default=500,
    )
    """The maximum number of peers per service"""

    # @model_validator(mode="after")
    # def pos_init(self) -> Self:
    #     self.logger = logger.bind(where="PeerRegistry")
    #     """The logger for the class"""
    #     return self

    def add_peer(self, peer: Peer) -> None:
        """Add a peer to the registry"""
        log = logger.bind(where=self.__class__.__name__, inside="add_peer", peer=peer)
        log.info("Adding a peer...")
        peers = self.services_and_peers.get(peer.service.service, [])

        log.bind(peers=peers).debug("List before adding the peer")
        peers.append(peer)
        self.services_and_peers[peer.service.service] = peers
        log.bind(peers=peers).debug("List after adding the peer")

        if len(peers) > self.max_peers_per_service:
            log.bind(peers=peers).info(
                f"Removing the first peer because the list exceeds the maximum capacity -> {self.max_peers_per_service}"
            )
            peers.pop(0)
            log.bind(
                peers=peers,
                why=f"Because the peers excedes the maximum capacity -> {self.max_peers_per_service}",
            ).debug("List after removing the first peer")

    def remove_peer(self, peer: Peer) -> None:
        """Remove a peer from the registry"""
        log = logger.bind(
            where=self.__class__.__name__, inside="remove_peer", peer=peer
        )
        log.info("Removing a peer")
        peers = self.services_and_peers.get(peer.service.service, [])
        try:
            log.bind(peers=peers).info("List before removing the peer")
            peers.remove(peer)
            log.bind(peers=peers).info("List after removing the peer")
        except Exception as e:
            log.error(f"Error trying to remove a peer that doesn't exist: {e}")
            raise e

    def get_peers(self, service_name: str, max_amount: Optional[int]) -> list[Peer]:
        """Get a list of the most recent joined peers from the registry"""
        log = logger.bind(
            where=self.__class__.__name__,
            inside="get_peer",
            service_name=service_name,
            max_amount=max_amount,
        )
        result = self.services_and_peers.get(service_name, [])
        log.info("Getting the list of peers")
        log.bind(peers=result).debug("List of peers before the execution of the method")
        if max_amount:
            result = result[-max_amount:]
        log.bind(peers=result).debug("List of peers after the execution of the method")
        return result

    def get_all_peers(self) -> list[Peer]:
        """Get all peers from the registry"""
        log = logger.bind(where=self.__class__.__name__, inside="get_all_peers")
        result = []
        for peers in self.services_and_peers.values():
            result.extend(peers)
        log.bind(all_peers=result).info("Getting all the peers in the register")
        return result
