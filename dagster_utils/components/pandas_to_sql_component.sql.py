from typing import Annotated, TYPE_CHECKING

import pandas as pd
import dagster as dg
from dagster import MaterializeResult, Definitions

from ..utils import cleanse_dataframe, write_dataframe_to_sql
from ..resources.connections import SqlTable, SqlConnectionResource, ConnectionResource
from .source import Source


def resolve_source(context: dg.ResolutionContext, source: Source):
    source_object_hook: type[Source] = Source.registry.get(source["type"])
    if source_object_hook is None:
        raise ValueError(f"Source type must be specified.")

    source_object = source_object_hook(**context.resolve_value(source, as_type=Source).model_dump())
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


if TYPE_CHECKING:
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


class PandasToSqlComponent(dg.Component, dg.Resolvable):
    source: Source
    target: SqlTable
    conn_info: ResolvedConnectionResource

    def build_defs(self, context: "ComponentLoadContext") -> Definitions:
        SourceType = self.source.registry.get(self.source.type)


        return Definitions(
            assets=[
                pands_to_sql_asset_factory(
                    input_path=self.input_path,
                    target_table=self.target_table,
                )(conn_info=self.conn_info),
            ],
        )


def pands_to_sql_asset_factory(input_path: str, target_table: SqlTable):
    """ Creates a simple EL pipeline that loads a pandas dataframe into a database.

    :param input_path: Path to the input CSV file.
    :param target_table: Target table in the database.
    :return: Dagster AssetsDefinition.
    """
    def _wrapped_asset_definition(conn_info: SqlConnectionResource):
        df = pd.read_excel(input_path)
        df = cleanse_dataframe(df)
        metadata = write_dataframe_to_sql(df, SqlWriteConfig(
            sql_table=target_table,
        ), conn_info)
        return MaterializeResult(
            metadata=metadata,
        )

    return _wrapped_asset_definition
