from basic_imports import *
from pydantic import BaseModel, Field, ValidationError
from pydantic.networks import IPvAnyAddress


class ServiceAnnouncement(BaseModel):
    """This class is used to store the configuration of a service"""

    service: str = Field(
        max_length=50,
        description="The name of the service",
        example="storage.chord.local",
    )
    """The name of the service"""
    ip: IPvAnyAddress = Field(description="The IP address of the service")
    """The IP address of the service"""
    port: int = Field(
        gt=0,
        lt=65536,
        description="The port number of the service",
    )
    """The port number of the service"""

    def to_json(self) -> str:
        """Convert the object to a JSON string"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str):
        """Create an instance of the class from a JSON string"""
        return cls.model_validate_json(json_str)
