import abc
import re
import time
from collections.abc import Callable
from typing import Any, ClassVar

import duckdb
import pandas as pd
import dagster as dg
import pyarrow
from dagster import AssetExecutionContext, MaterializeResult, asset, OpExecutionContext
from pydantic import BaseModel, Field
from sqlalchemy import String, text
from sqlalchemy.sql.type_api import TypeEngine

from .pipeline import SqlTable, SqlDestination, SqlConnectionResource, GlobalConfigResource, Source
from .sources import CsvSource, ExcelSource, ParquetSource, JsonSource, CsvReadOptionsDuckdb, CsvReadOptionsPandas
from ..utils import Registry


class Timer(object):
    _start_time: float
    _end_time: float
    _duration: float

    def __init__(self):
        pass

    def __enter__(self):
        self._start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._time = time.time()
        self._duration = self._time - self._start_time
        return exc_type is None

    @property
    def start_time(self) -> float:
        return self._start_time

    @property
    def end_time(self) -> float:
        return self._end_time

    @property
    def duration(self) -> float:
        return self._duration


class Task(BaseModel):
    """ Base class for all tasks.

    All sources should inherit from this class to ensure they are registered with the package registry.
    """
    name: str
    compute_fn: Callable
    key_prefix: str | list[str] = Field(default_factory=list)
    key: str | None = None
    description: str | None = None
    group_name: str | None = None
    # metadata: dict[str, MetadataValue | TableSchema | TableColumnLineage | AssetKey | PathLike | dict | float | int | list | str | datetime | None] | None = None
    # io_manager_key: str | None = None
    # io_manager_def: object | None = None
    # required_resource_keys: AbstractSet[str] | None = None
    # resource_defs: dict[str, ResourceDefinition] | None = None
    # partitions_def: PartitionsDefinition | None = None
    # automation_condition: AutomationCondition | None = None
    op_tags: dict[str, Any] | None = None
    tags: dict[str, str] | None = None

    registry: ClassVar[Registry["Task"]] = Registry()


class SourceReader(abc.ABC):
    @property
    @abc.abstractmethod
    def compute_fn_name(self) -> str:
        pass

    def __init__(self, source: Source):
        if not isinstance(source, Source):
            raise ValueError(f"Expected a Source object, got {type(source)}")
        else:
            self.source = source

    @classmethod
    def register(cls, fn: Callable[Source, Any] | None = None, *, name: str | None = None) -> type["SourceReader"]:
        class _SourceReader(SourceReader):
            compute_fn_name = name or fn.__name__
            compute_fn = fn

        return _SourceReader


def cleanse_obj_name(obj_name: str, ignore_dot: bool = False):
    """ Standardise SQL object name. Replaces consecutive whitespaces as underscores and removes all other non-alphanumerics.

    :param obj_name: Name of object.
    :param ignore_dot: Ignore dot. This is sometimes useful when cleansing multipart schema names.
    :return: Cleansed object name.
    """
    # Remove special characters first, then replace consecutive whitespaces / dashes to underscore.
    if ignore_dot:
        obj_name = re.sub(r"[^\w\s_.\-]", "", obj_name)
        return re.sub(r"[\-\s_]+", "_", obj_name).lower()
    else:
        obj_name = re.sub(r"[^\w\s_\-]", "", obj_name)
        return re.sub(r"[.\-\s_]+", "_", obj_name).lower()


def _cleanse_column_names(df: pd.DataFrame):
    return df.rename(columns={
        x: cleanse_obj_name(x)
        for x in df.columns
    })


def _cleanse_string_columns(df: pd.DataFrame):
    str_cols = df.select_dtypes('object').columns
    df[str_cols] = df[str_cols].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def auto_field(df: pd.DataFrame) -> dict[str, TypeEngine]:
    """ Automatically assign size limits to string fields for SQL consumption.

    :param df: dataframe.
    :return: dataframe with size limits.
    """
    result = {}
    for x in df.select_dtypes('object').columns:
        string_max_len = int(df[x].astype(str).str.len().max())

        # Some SQL db does not support varchar with a length above 8000.
        if string_max_len > 8000:
            result[x] = String()
        else:
            result[x] = String(string_max_len)

    return result


def cleanse_dataframe(df: pd.DataFrame, standardise_column_names: bool = True, cleanse_string_columns: bool = True):
    """ Cleanse the input dataframe and perform auto field sizing.

    :param df: input dataframe.
    :param standardise_column_names: Whether to standardise column names.
    :param cleanse_string_columns: Whether to perform a default cleansing.
    :return: Cleansed dataframe.
    """
    if standardise_column_names:
        df = _cleanse_column_names(df)
    if cleanse_string_columns:
        df = _cleanse_string_columns(df)
    return df


def copy_table(
        source_table: SqlTable,
        target_table: SqlTable,
        conn_info: SqlConnectionResource,
        global_config: GlobalConfigResource
):
    """ Move SQL table within the same database. Returns standard metadata dictionary for convenience.

    :param source_table: Source table.
    :param target_table: Target table.
    :param conn_info: Connection info.
    """
    engine = conn_info.get_engine()
    with engine.connect() as conn:
        # Source table is assumed to not require schema alteration.
        source_table_clause = source_table.get_table_name()
        target_table_clause = target_table.get_table_name(schema_postfix=global_config.schema_postfix)
        conn.execute(text(f"""DROP TABLE IF EXISTS {target_table_clause}"""))
        res = conn.execute(text(f"""SELECT * INTO {target_table_clause} FROM {source_table_clause}"""))
        conn.commit()
        return {
            "dagster/row_count": res.rowcount,
            "dagster/relation_identifier": target_table.get_table_name(schema_postfix=global_config.schema_postfix),
        }


PandasDataFrameTransform = Callable[[pd.DataFrame, AssetExecutionContext, GlobalConfigResource], pd.DataFrame]


def log_pandas_read_status(context: dg.OpExecutionContext, output: pd.DataFrame):
    context.log.info(f"Successfully read {len(output.index)} lines of data from source location")


def log_duckdb_read_status(context: dg.OpExecutionContext, output: duckdb.DuckDBPyRelation):
    context.log.info(f"Successfully read {len(output)} lines of data from source location")


def task(
        fn: Callable | None = None,
        *,
        name: str | None = None
):
    name = name or fn.__name__

    if fn is not None:
        task = Task(
            compute_fn=fn,
            name=name,
        )
        Task.registry.register(name, task)
        return fn

    def _inner(_fn: Callable) -> Callable:
        task = Task(
            compute_fn=_fn,
            name=name,
        )
        return _fn

    return _inner


def pipeline(
        fn: Callable | None = None,
        *,
        name: str | None = None,
        backend: str | None = None
):
    name = name or fn.__name__

    if fn is not None:
        # TODO: Register this somewhere
        task = Task(
            compute_fn=fn,
            name=name,
        )
        return fn

    def _inner(_fn: Callable) -> Task:
        task = Task(
            compute_fn=_fn,
            name=name,
        )
        return _fn

    return _inner


@task
def read_csv_to_df(source: CsvSource, context: dg.OpExecutionContext, global_config: GlobalConfigResource):
    output = pd.read_csv(source.path, **source.read_options.model_dump(exclude_unset=True))
    log_pandas_read_status(context, output)
    return output


@task
def read_csv_to_duckdb(source: CsvSource, context: dg.OpExecutionContext, global_config: GlobalConfigResource):
    output = duckdb.read_csv(source.path, **source.read_options.model_dump(exclude_unset=True))
    log_duckdb_read_status(context, output)
    return output


@task
def read_excel_to_pandas(source: ExcelSource, context: dg.OpExecutionContext, global_config: GlobalConfigResource):
    args = source.read_options.model_dump(exclude_unset=True)
    if source.sheet_name:
        args["sheet_name"] = source.sheet_name
    output = pd.read_excel(source.path, **args)

    log_pandas_read_status(context, output)
    return output


@task
def read_json_to_pandas(source: JsonSource, context: dg.OpExecutionContext, global_config: GlobalConfigResource):
    output = pd.read_json(source.path, **source.read_options.model_dump(exclude_unset=True))
    log_pandas_read_status(context, output)
    return output


@task
def read_json_to_duckdb(source: JsonSource, context: dg.OpExecutionContext, global_config: GlobalConfigResource):
    output = duckdb.read_json(source.path, **source.read_options.model_dump(exclude_unset=True))
    log_duckdb_read_status(context, output)
    return output


@task
def read_parquet_to_pandas(source: ParquetSource, context: dg.OpExecutionContext, global_config: GlobalConfigResource):
    output = pd.read_parquet(source.path, **source.read_options.model_dump(exclude_unset=True))
    log_pandas_read_status(context, output)
    return output


@task
def read_parquet_to_duckdb(source: ParquetSource, context: dg.OpExecutionContext, global_config: GlobalConfigResource):
    output = duckdb.read_parquet(source.path, **source.read_options.model_dump(exclude_unset=True))
    log_duckdb_read_status(context, output)
    return output


@task
def read_duckdb_to_postgres(source: DuckdbSource, context: dg.OpExecutionContext, global_config: GlobalConfigResource):
    duck_table = duckdb.read_parquet(source)
    duck_table.execute()

    duckdb.sql(
        "INSTALL postgres;"
        "ATTACH 'postgresql://postgres:postgres@localhost/postgres' AS pg (TYPE postgres);"
        f"INSERT INTO pg.data SELECT * FROM '{file.resolve().absolute()}';"
    )


@pipeline
def cleanse_pandas(df: pd.DataFrame):
    def _wrapped_function(context: dg.OpExecutionContext, global_config: GlobalConfigResource):
        return cleanse_dataframe(df)

    return _wrapped_function


@task
def write_pandas_to_sql(df: pd.DataFrame, destination: SqlDestination, conn_info: SqlConnectionResource,
                        global_config: GlobalConfigResource):
    """ Write the provided dataframe to SQL table. Returns standard metadata for convenience.

    :param df: The dataframe to ingest.
    :param destination: Additional configuration for writing table to SQL.
    :param conn_info: Connection information.
    :param global_config: Global configuration for the project.
    """
    with Timer() as timer:
        schema = destination.sql_table.schema + global_config.schema_postfix
        table_name = destination.sql_table.name

        # Modify various object names per configuration
        if destination.write_options.to_lower_schema:
            schema = cleanse_obj_name(schema, ignore_dot=True)
        if destination.write_options.to_lower_table:
            table_name = cleanse_obj_name(table_name)

        if destination.write_options.cleanse_column_name:
            df = df.rename(columns=cleanse_obj_name)
        if destination.write_options.auto_field and len(df.index) > 0:
            dtype = auto_field(df)
        else:
            dtype = None

        engine = conn_info.get_engine()
        if destination.write_options.direct_write:
            df.to_sql(f"{schema}.{table_name}", con=engine, index=False, if_exists="replace",
                      chunksize=destination.write_options.chunksize)
        else:
            df.to_sql(table_name, schema=schema, con=engine, index=False, if_exists="replace",
                      chunksize=destination.write_options.chunksize, method=destination.write_options.method,
                      dtype=dtype)
    return {
        "Execution Duration": timer.duration,
        "dagster/row_count": len(df),
        "dagster/relation_identifier": destination.sql_table.get_table_name(
            schema_postfix=global_config.schema_postfix),
    }


@task
def write_duckdb_to_sql(
        table: duckdb.DuckDBPyRelation,
        destination: SqlDestination,
        conn_info: SqlConnectionResource,
        global_config: GlobalConfigResource
):
    with Timer() as timer:
        schema = destination.sql_table.schema + global_config.schema_postfix
        table_name = destination.sql_table.name

        # Modify various object names per configuration
        if destination.write_options.to_lower_schema:
            schema = cleanse_obj_name(schema, ignore_dot=True)
        if destination.write_options.to_lower_table:
            table_name = cleanse_obj_name(table_name)
        if destination.write_options.cleanse_column_name:
            table: pyarrow.Table = table.to_arrow_table()
            table.rename_columns(names={
                x: cleanse_obj_name(x)
                for x in table.columns
            })

        engine = conn_info.get_engine()
        table.to_sql(f"{schema}.{table_name}", con=engine, index=False, if_exists="replace",
                     chunksize=destination.write_options.chunksize)
    return {
        "Execution Duration": timer.duration,
        "dagster/row_count": len(df),
        "dagster/relation_identifier": destination.sql_table.get_table_name(
            schema_postfix=global_config.schema_postfix),
    }


@pipeline(name="csv_to_sql", backend="pandas")
def csv_to_sql_pandas(
        source: CsvSource | ExcelSource,
        destination: SqlDestination,
        config: CsvReadOptionsPandas,
        context: OpExecutionContext,
        global_config: GlobalConfigResource,
        conn_info: SqlConnectionResource
):
    df = read_csv_to_df(source=source, context=context, global_config=global_config)
    # df = cleanse_dataframe(df)
    metadata = write_pandas_to_sql(df, destination, conn_info, global_config=global_config)

    return MaterializeResult(
        df,
        metadata=metadata,
    )


@pipeline(name="csv_to_sql", backend="duckdb")
def csv_to_sql_duckdb(
        source: CsvSource,
        destination: SqlDestination,
        config: CsvReadOptionsDuckdb,
        context: OpExecutionContext,
        global_config: GlobalConfigResource,
        conn_info: SqlConnectionResource
):
    table = read_csv_to_duckdb(source=source, context=context, global_config=global_config)

    # for transform in transforms:
    #     df = df.pipe(transform, context=context, global_config=global_config)
    metadata = write_duckdb_to_sql(table, destination, conn_info, global_config=global_config)

    return MaterializeResult(
        table,
        metadata=metadata,
    )


@task
@pipeline(name="excel_to_sql", backend="pandas")
def excel_to_sql_pandas(
        source: ExcelSource,
        destination: SqlDestination,
        config: CsvReadOptionsDuckdb,
        context: OpExecutionContext,
        global_config: GlobalConfigResource,
        conn_info: SqlConnectionResource
):
    df = read_excel_to_pandas(source=source, config=config, context=context, global_config=global_config)

    # for transform in transforms:
    #     df = df.pipe(transform, context=context, global_config=global_config)
    # df = cleanse_dataframe(df)
    metadata = write_pandas_to_sql(df, destination, conn_info, global_config=global_config)

    return MaterializeResult(
        df,
        metadata=metadata,
    )


@pipeline
def sql_copy(source_table: SqlTable, target_table: SqlTable, context: AssetExecutionContext,
             global_config: GlobalConfigResource, conn_info: SqlConnectionResource):
    metadata = copy_table(source_table, target_table, conn_info, global_config)
    return MaterializeResult(
        metadata=metadata,
    )


def parse_task(dct: dict[str, Any]) -> Task:
    task: Task = Task.registry.get(dct["type"])
    if task is None:
        raise ValueError(f"Task type must be specified.")

    return task


def parse_asset(dct: dict[str, Any]) -> dg.AssetAsset:
    for task_dict in dct["tasks"]:
        task = parse_task(task_dict)

    # TODO: To determine appropriate backend. Dagster Op backend has massive resource cost

    #     name: jaffle_shop_customers
    #     description: ''
    #     resources:
    #       - conn_info\    #     deps:
    #       - jaffle_shop
    #     tasks:
    #       - type: read_csv_to_df
    #         # TODO: Consider dependency modelling
    #         source:
    #           - jaffle_shop
    #       - type: write_df_to_sql
    #         sql_table:
    #           schema: public
    #           name: jaffle_shop
    #         conn_info: conn_info

    return dg.asset(
        name=dct["name"],
        description=dct["description"],
    )
