from typing import Annotated, TYPE_CHECKING

import dagster as dg

from ..resources.connections import ConnectionResource
from .source import Source, Target


def resolve_source(context: dg.ResolutionContext, source: Source):
    source_object_hook: type[Source] = Source.registry.get(source["type"])
    if source_object_hook is None:
        raise ValueError(f"Source type must be specified.")

    source_object = source_object_hook(**context.resolve_value(source, as_type=Source).model_dump())
    return source_object


def resolve_target(context: dg.ResolutionContext, target: Target):
    target_object_hook: type[Target] = Target.registry.get(target["type"])
    if target_object_hook is None:
        raise ValueError(f"Target type must be specified.")


def resolve_connection(
        context: dg.ResolutionContext,
        raw_connection: dict,
) -> ConnectionResource:
    connection_type = raw_connection["type"]
    ConnectionType = ConnectionResource.registry[connection_type]

    return ConnectionType.model_validate(
        context.resolve_value(raw_connection, as_type=dict),
    )


# the yaml field will be a string, which is then parsed into a datetime object
ResolvedSource = Annotated[
    Source,
    dg.Resolver(resolve_source, model_field_type=dict),
]
ResolvedTarget = Annotated[
    Target,
    dg.Resolver(resolve_target, model_field_type=dict),
]
ResolvedConnectionResource = Annotated[
    ConnectionResource,
    dg.Resolver(resolve_connection, model_field_type=dict),
]


class ELModelSpec(dg.Model):
    source: ResolvedSource
    destination: ResolvedTarget
    backend: str | None = dg.Field(
        None,
        description="The backend to use for the operation.",
    )


class Connections(dg.Model):
    source: ResolvedConnectionResource = dg.Field(
        description="The source connection to use."
    )
    destination: ResolvedConnectionResource = dg.Field(
        description="The destination connection to use."
    )
    file_name: str = Field(
        description="Path to the csv file.",
    )
    sql_tables: SqlTable = Field(
        description="The table to read from."
    )
    chunksize: int = Field(
        200000,
        description="Size of each chunk. A lower number improves memory efficiency while a higher number improves performance.",
    )
    args: dict[str, Any] = Field(
        default_factory=lambda: {},
        description=".",
    )


class DumpTableOpConfig(dg.Model):
    tables: list[Annotated[Union[DumpTableToCsv, DumpTableToExcel], Field(discriminator="format")]] = Field(
        description="A list of tables to dump",
    )


class ELModel(dg.Model):
    """MODEL SUMMARY HERE."""
    description: str | None
    source: Source
    destination: Destination
    # description: ''
    # source: jaffle_shop
    # destination:
    #   type: sql
    #   sql_table:
    #     schema: public
    #     name: jaffle_shop
    #     conn_info: conn_info


class ELComponent(dg.Component, dg.Model, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """
    connections: Connections
    el_models: list[ELModelSpec]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _assets = []

        for model in self.el_models:
            asset_key = f"{model.source.name}_to_{model.destination.name}".replace(".", "_")

            el_function = ELModel.find(model.source.type, model.destination.type)
            asset = dg.asset(compute_fn=el_function, name=asset_key)

            _assets.append(asset)

        resources = {
            "source_connection": self.connections.source,
            "target_connection": self.connections.destination,
        }

        return dg.Definitions(assets=_assets, resources=resources)
