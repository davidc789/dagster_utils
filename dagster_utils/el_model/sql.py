from typing import Any

import dagster as dg
import sqlalchemy

from .el_model import ELModelImplementation, ELModelSpec
from ..resources.sql import SqlConnectionResource


class CopyTableConfig(dg.ConfigurableResource):
    read_options: dict[str, Any]
    write_options: dict[str, Any]

@ELModelImplementation.register
def copy_table(
        context: dg.AssetExecutionContext,
        el_model: ELModelSpec[S],
):
    """ Move SQL table within the same database. Returns standard metadata dictionary for convenience.

    :param source_spec: Source table.
    :param target_spec: Target table.
    :param source_connection: Connection info.
    :param target_connection: Connection info.
    """
    source_connection = el_model.source.connection

    if source_connection.url != target_connection.url:
        raise ValueError("Cannot copy tables across different databases or with different credentials.")

    engine = source_connection.get_engine()

    source_table_clause = source_spec.get_table_name()
    target_table_clause = target_spec.get_table_name()

    with engine.connect() as conn:
        conn.execute(sqlalchemy.sql.text(f"""DROP TABLE IF EXISTS {target_table_clause}"""))
        res = conn.execute(sqlalchemy.sql.text(f"""SELECT * INTO {target_table_clause} FROM {source_table_clause}"""))
        conn.commit()
        return {
            "dagster/row_count": res.rowcount,
            "dagster/relation_identifier": target_spec.get_table_name(),
        }

