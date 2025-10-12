import re
import time
from collections.abc import Callable
from typing import overload

import duckdb
import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult
from sqlalchemy import String, text
from sqlalchemy.sql.type_api import TypeEngine

from .resources import SqlTable, CsvSource, ExcelSource, SqlDestination, JsonSource, ParquetSource, CsvReadOptions, \
    CsvReadOptionsDuckdb, ParquetReadOptions, JsonReadOptions, ExcelReadOptions, JsonReadOptionsDuckdb, \
    ParquetReadOptionsDuckdb
from pipeline.pipeline.sql_resources import SqlConnectionResource, GlobalConfigResource


def parse_model():
    pass


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


def write_dataframe_to_sql(df: pd.DataFrame, config: SqlDestination, conn_info: SqlConnectionResource, global_config: GlobalConfigResource):
    """ Write the provided dataframe to SQL table. Returns standard metadata for convenience.

    :param df: The dataframe to ingest.
    :param config: Additional configuration for writing table to SQL.
    :param conn_info: Connection information.
    :param global_config: Global configuration for the project.
    """
    start = time.time()
    schema = config.sql_table.schema + global_config.schema_postfix
    table_name = config.sql_table.name

    # Modify various object names per configuration
    if config.write_options.to_lower_schema:
        schema = cleanse_obj_name(schema, ignore_dot=True)
    if config.write_options.to_lower_table:
        table_name = cleanse_obj_name(table_name)

    if config.write_options.cleanse_column_name:
        df = df.rename(columns=cleanse_obj_name)
    if config.write_options.auto_field and len(df.index) > 0:
        dtype = auto_field(df)
    else:
        dtype = None

    engine = conn_info.get_engine()
    if config.write_options.direct_write:
        df.to_sql(f"{schema}.{table_name}", con=engine, index=False, if_exists="replace",
                  chunksize=config.write_options.chunksize)
    else:
        df.to_sql(table_name, schema=schema, con=engine, index=False, if_exists="replace",
                  chunksize=config.write_options.chunksize, method=config.write_options.method, dtype=dtype)
    end = time.time()

    return {
        "Execution Duration": end - start,
        "dagster/row_count": len(df),
        "dagster/relation_identifier": config.sql_table.get_table_name(schema_postfix=global_config.schema_postfix),
    }


def copy_table(source_table: SqlTable, target_table: SqlTable, conn_info: SqlConnectionResource,
               global_config: GlobalConfigResource):
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


@overload
def read_file(source: CsvSource[CsvReadOptions] | ExcelSource | JsonSource[JsonReadOptions] | ParquetSource[ParquetReadOptions]) -> pd.DataFrame: ...
@overload
def read_file(source: CsvSource[CsvReadOptionsDuckdb] | JsonSource[JsonReadOptionsDuckdb] | ParquetSource[ParquetReadOptionsDuckdb]) -> duckdb.DuckDBPyRelation: ...
def read_file(source: CsvSource | ExcelSource | JsonSource | ParquetSource):
    def _wrapped_function(context: AssetExecutionContext, global_config: GlobalConfigResource):
        if isinstance(source, CsvSource):
            if source.read_options.backend == "pandas":
                output = pd.read_csv(source.path, **source.read_options.model_dump(exclude_unset=True))
            else:
                output = duckdb.read_csv(source.path, **source.read_options.model_dump(exclude_unset=True))
        elif isinstance(source, ExcelSource):
            args = source.read_options.model_dump(exclude_unset=True)
            if source.sheet_name:
                args["sheet_name"] = source.sheet_name
            output = pd.read_excel(source.path, **args)
        elif isinstance(source, JsonSource):
            if source.read_options.backend == "pandas":
                output = pd.read_json(source.path, **source.read_options.model_dump(exclude_unset=True))
            else:
                output = duckdb.read_json(source.path, **source.read_options.model_dump(exclude_unset=True))
        elif isinstance(source, ParquetSource):
            if source.read_options.backend == "pandas":
                output = pd.read_json(source.path, **source.read_options.model_dump(exclude_unset=True))
            else:
                output = duckdb.read_csv(source.path, **source.read_options.model_dump(exclude_unset=True))

        if source.read_options.backend == "pandas":
            context.log.info(f"Successfully read {len(output.index)} lines of data from source location")
        else:
            context.log.info(f"Successfully read {len(output)} lines of data from source location")

        return output

    return _wrapped_function


def cleanse_pandas(df: pd.DataFrame):
    def _wrapped_function(context: AssetExecutionContext, global_config: GlobalConfigResource):
        return cleanse_dataframe(df)

    return _wrapped_function


def to_sql(df: pd.DataFrame, config: SqlDestination):
    def _wrapped_function(context: AssetExecutionContext, conn_info: SqlConnectionResource, global_config: GlobalConfigResource):
        """ Write the provided dataframe to SQL table. Returns standard metadata for convenience.

        :param df: The dataframe to ingest.
        :param config: Additional configuration for writing table to SQL.
        :param conn_info: Connection information.
        :param global_config: Global configuration for the project.
        """
        start = time.time()
        schema = config.sql_table.schema + global_config.schema_postfix
        table_name = config.sql_table.name

        # Modify various object names per configuration
        if config.write_options.to_lower_schema:
            schema = cleanse_obj_name(schema, ignore_dot=True)
        if config.write_options.to_lower_table:
            table_name = cleanse_obj_name(table_name)

        if config.write_options.cleanse_column_name:
            df = df.rename(columns=cleanse_obj_name)
        if config.write_options.auto_field and len(df.index) > 0:
            dtype = auto_field(df)
        else:
            dtype = None

        engine = conn_info.get_engine()
        if config.write_options.direct_write:
            df.to_sql(f"{schema}.{table_name}", con=engine, index=False, if_exists="replace",
                      chunksize=config.write_options.chunksize)
        else:
            df.to_sql(table_name, schema=schema, con=engine, index=False, if_exists="replace",
                      chunksize=config.write_options.chunksize, method=config.write_options.method, dtype=dtype)
        end = time.time()

        return {
            "Execution Duration": end - start,
            "dagster/row_count": len(df),
            "dagster/relation_identifier": config.sql_table.get_table_name(schema_postfix=global_config.schema_postfix),
        }

    return _wrapped_function


def csv_to_sql(
        source: CsvSource | ExcelSource,
        destination: SqlDestination,
        # transforms: list[PandasDataFrameTransform],
):
    def _wrapped_function(context: AssetExecutionContext, global_config: GlobalConfigResource, conn_info: SqlConnectionResource):
        if isinstance(source, CsvSource):
            df = pd.read_csv(source.path, **source.read_options.model_dump(exclude_unset=True))
        elif isinstance(source, ExcelSource):
            args = source.read_options.model_dump(exclude_unset=True)
            if source.sheet_name:
                args["sheet_name"] = source.sheet_name
            df = pd.read_excel(source.path, **args)
        else:
            raise ValueError(f"Unsupported file type for '{source.path}'")
        context.log.info(f"Successfully read {len(df)} lines of data from source location")
        # for transform in transforms:
        #     df = df.pipe(transform, context=context, global_config=global_config)
        df = cleanse_dataframe(df)
        metadata = write_dataframe_to_sql(df, destination, conn_info, global_config=global_config)
        return MaterializeResult(
            metadata=metadata,
        )

    return _wrapped_function


def sql_copy(source_table: SqlTable, target_table: SqlTable):
    def _wrapped_asset_definition(context: AssetExecutionContext, global_config: GlobalConfigResource, conn_info: SqlConnectionResource):
        metadata = copy_table(source_table, target_table, conn_info, global_config)
        return MaterializeResult(
            metadata=metadata,
        )

    return _wrapped_asset_definition
