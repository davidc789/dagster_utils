import shutil
from pathlib import Path

import pandas as pd
from dagster import op, OpExecutionContext, PipesSubprocessClient
from sqlalchemy import text

from .resources import SqlConnectionResource, SqlTable, ArchiveTableOpConfig, PrepareDevOpConfig
from dagster_utils.utils import add_date_time_stamps_to_suffix


@op
def dump_tables(context: OpExecutionContext, tables: list[SqlTable], conn_info: SqlConnectionResource, path: str):
    """ Dump SQL tables to a CSV file.

    :param context: Dagster Op execution context.
    :param tables: Dump all the tables into CSV files.
    :param conn_info: Connection information.
    :param path: Directory to dump the tables.
    """
    for tbl in tables:
        path_name = Path(path) / f"{tbl.schema.replace('.', '_')}_{tbl.name}.csv"
        (
            pd
            .read_sql_table(tbl.name, schema=tbl.schema, con=conn_info.get_engine())
            .to_csv(path_name, index=False)
        )


@op
def archive_file(context: OpExecutionContext, file_path: str, archive_directory: str, suffix: str = "",
                 datestamp: bool = True, timestamp: bool = True):
    """ Archive a file with optional suffix and timestamps.

    :param context: Dagster Op execution context.
    :param file_path: Path to the file to archive.
    :param archive_directory: Directory to archive the file.
    :param suffix: Suffix to include after the file. Default to empty string.
    :param datestamp: Whether to include a date suffix in the format of _YYYYMMDD. Defaults to True.
    :param timestamp: Whether to include a time suffix in the format of _HHMMSS. Defaults to True.
    """
    suffix = add_date_time_stamps_to_suffix(suffix, datestamp, timestamp)
    context.log.info(f"Archiving {file_path} to {archive_directory}")
    shutil.copy(
        file_path, Path(archive_directory) / f"{Path(file_path).name}{suffix}{'.'.join(Path(file_path).suffixes)}")


@op
def git_fetch(context: OpExecutionContext, subprocess_client: PipesSubprocessClient):
    yield subprocess_client.run(
        command=["git", "fetch", "origin"],
        context=context,
    ).get_results()


@op
def git_pull(context: OpExecutionContext, subprocess_client: PipesSubprocessClient):
    """ Pull any git repository updates.

    :param context: Dagster Op execution context.
    """
    yield subprocess_client.run(
        command=["git", "pull", "origin", "master"],
        context=context,
    ).get_results()


@op
def archive_tables(context: OpExecutionContext, config: ArchiveTableOpConfig, conn_info: SqlConnectionResource):
    """ Archive a list of selected table with optional suffix and timestamps.

    Warning: Although validations are imposed, there may still a potential SQL injection risk. Do not expose to untrusted users.

    :param context: Op execution context.
    :param config: Archive configuration. Refer to the config class for details.
    :param conn_info: Connection information.
    """
    suffix = add_date_time_stamps_to_suffix(config.suffix, config.datestamp, config.timestamp)
    engine = conn_info.get_engine()

    with engine.connect() as conn:
        for tbl in config.tables:
            if config.archive_schema is None:
                archive_schema = tbl.schema
            else:
                archive_schema = config.archive_schema

            original_table_name = tbl.get_table_name()
            archive_table_name = SqlTable(
                schema=archive_schema,
                name=f"{config.prefix}{tbl.schema.replace(".", "_")}_{tbl.name}{suffix}",
            ).get_table_name()
            context.log.info(f'Archiving "{original_table_name}" as "{archive_table_name}"')
            conn.execute(text(f"""
                select * into {archive_table_name} from {original_table_name}
            """))


@op
def prepare_dev_tables_op(context: OpExecutionContext, config: PrepareDevOpConfig, conn_info: SqlConnectionResource):
    """ Archive a list of selected table with optional suffix and timestamps.

    Warning: Although validations are imposed, there may still be a potential SQL injection risk.
    Do not expose it to untrusted users.

    :param context: Op execution context.
    :param config: Archive configuration. Refer to the config class for details.
    :param conn_info: Connection information.
    """
    engine = conn_info.get_engine()

    with engine.connect() as conn:
        for tbl in config.tables:
            original_table_name = tbl.get_table_name()
            dev_table = SqlTable(
                schema=tbl.schema + config.schema_suffix,
                name=tbl.name + config.name_suffix,
            )
            dev_table_name = dev_table.get_table_name()

            # Prepare to migrate the original table to dev table. Drop first and re-create later.
            context.log.info(f'Dropping "{dev_table_name}" if exists')
            conn.execute(dev_table.get_drop_sql())
            context.log.info(f'Copying "{original_table_name}" as "{dev_table_name}"')
            conn.execute(text(f"""
                select * into {dev_table_name} from {original_table_name}
            """))


@op
def run_sql_script(conn_info: SqlConnectionResource, script_path: str):
    """ Runs SQL script at the given path asynchronously.

    This is best used for running non-asset related database maintenance tasks.

    :param conn_info: Connection information.
    :param script_path: Path to the SQL script.
    """
    with conn_info.get_engine().connect() as conn, open(script_path) as file:
        for query in file.read().split(";"):
            query = query.strip()
            if query:
                conn.execute(text(query))


resources = {
    "subprocess_client": PipesSubprocessClient(
        cwd=".",
    ),
}
