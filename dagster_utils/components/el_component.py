from typing import Annotated

import dagster as dg

from ..resources.abc import ConnectionResource, DataSpecResource
from ..el_model.el_model import ELModel, ELModelSpecResource


def resolve_source_target(context: dg.ResolutionContext, source: DataSpecResource):
    source_object_hook: type[DataSpecResource] = DataSpecResource.registry.get(source["type"])
    if source_object_hook is None:
        raise ValueError(f"Source type must be specified.")

    source_object = source_object_hook(**context.resolve_value(source, as_type=DataSpecResource).model_dump())
    return source_object


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
ResolvedSourceTarget = Annotated[
    DataSpecResource,
    dg.Resolver(resolve_source_target, model_field_type=dict),
]
ResolvedConnectionResource = Annotated[
    ConnectionResource,
    dg.Resolver(resolve_connection, model_field_type=dict),
]


class ELModelSpec(dg.Model):
    source: ResolvedSourceTarget
    target: ResolvedSourceTarget
    backend: str | None = dg.Field(
        None,
        description="The backend to use for the operation.",
    )


class Connections(dg.Model):
    source: ResolvedConnectionResource = dg.Field(
        description="The source connection to use."
    )
    target: ResolvedConnectionResource = dg.Field(
        description="The destination connection to use."
    )


class ELComponent(dg.Component, dg.Model, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """
    connections: Connections
    el_models: list[ELModelSpec]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _assets = []

        for model in self.el_models:
            asset_key = f"{model.source.name}_to_{model.target.name}".replace(".", "_")

            el_function = ELModel.find(model.source.type, model.target.type)
            asset = dg.asset(compute_fn=el_function, name=asset_key)

            _assets.append(asset)

        resources = {
            "el_model": self.connections.source,
            "target_connection": self.connections.target,
        }

        return dg.Definitions(assets=_assets, resources=resources)
