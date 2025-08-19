import datetime

from pydantic import BaseModel


class DataProfile(BaseModel):
    version: int
    branch_name: str
    timestamp: datetime.datetime


class AssetOptions(BaseModel):
    """ A base class for adding options.

    All sources should inherit from this class to ensure they are registered with the package registry.
    """
    name: str
    type: str
    backend: str
    key_prefix: str | list[str]
    description: str | None = None
    group_name: str | None = None
    # metadata: dict[str, MetadataValue | TableSchema | TableColumnLineage | AssetKey | PathLike | dict | float | int | list | str | datetime | None] | None = None
    # io_manager_key: str | None = None
    # io_manager_def: object | None = None
    # required_resource_keys: AbstractSet[str] | None = None

    # resource_defs: dict[str, ResourceDefinition] | None = None
    # partitions_def: PartitionsDefinition | None = None
    automation_condition: AutomationCondition | None = None
    op_tags: dict[str, Any] | None = None
    tags: dict[str, str] | None = None
