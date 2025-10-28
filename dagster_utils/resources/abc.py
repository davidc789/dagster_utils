from typing import TYPE_CHECKING

import dagster as dg
from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field, ConfigDict, PrivateAttr
from sqlalchemy import MetaData
from abc import ABC

from ..utils import RegisterSubclass

if TYPE_CHECKING:
    from typing import Literal

__all__ = ["ConnectionResource", "MetaDataResource", "LocalConnectionResource"]


class ConnectionResource(ConfigurableResource, ABC, RegisterSubclass):
    """ Base class for resources managing connections. """
    type: str


class MetaDataResource(ConfigurableResource):
    schema: str | None = Field(
        default=None,
        title="Schema",
        description="The default schema to use.",
    )
    quote_schema: bool | None = Field(
        default=None,
        title="Qute Schema",
        description="Whether to quote schema and table names for object in the metadata.",
    )
    info: dict | None = Field(
        default=None,
        title="Info",
        description="Additional information passed onto the table API.",
    )

    _metadata: MetaData | None = PrivateAttr(None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._metadata = MetaData(schema=self.schema, quote_schema=self.quote_schema, info=self.info)

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        super().teardown_after_execution(context)

        if self._metadata is not None:
            self._metadata = None

    def get_metadata(self) -> MetaData:
        if self._metadata is None:
            raise ValueError("MetaData not initialised properly.")

        return self._metadata


class LocalConnectionResource(ConnectionResource):
    """ Specifies a local connection resource. """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: Literal["local"] = "local"
    url: str = Field(
        default="file://%USERPROFILE%", description="The local path to the file."
    )


class DataSpecResource(dg.ConfigurableResource, ABC, RegisterSubclass):
    """ Base class for all sources or targets.

    All sources and targets should inherit from this class to ensure they are registered with the package registry.
    """
    type: str
    name: str


class DataResource(dg.ConfigurableResource, ABC):
    """ A wrapper for a spec and connection. """
    connection: ConnectionResource
    spec: DataSpecResource


class SourceReadConfig(dg.Config, ABC):
    pass
