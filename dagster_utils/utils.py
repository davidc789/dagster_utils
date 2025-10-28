import datetime
import re
import time
from pathlib import Path
from collections.abc import Collection
from typing import TYPE_CHECKING

import dagster
import pandas as pd
# import geopandas as gpd
from dagster import AssetSpec, AssetsDefinition, MaterializeResult
from sqlalchemy import String, text
from sqlalchemy.sql.type_api import TypeEngine

from .resources.abc import SqlTable, SqlWriteConfig
from .resources.sql import SqlConnectionResource

if TYPE_CHECKING:
    from typing import Callable

PathLike = str | Path


def cleanse_obj_name(obj_name: str, ignore_dot: bool = False):
    """ Standardise SQL object name. Replaces consecutive whitespaces as underscores and removes all other non-alphanumerics.

    :param obj_name: Name of the object.
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


def add_date_time_stamps_to_suffix(suffix: str, datestamp: bool = True, timestamp: bool = True,
                                   dt_override: datetime.datetime = None):
    """ Adds datestamps and timestamps to the suffix if requested.

    :param suffix: Original suffix.
    :param datestamp: Whether to include a date suffix in the format of _YYYYMMDD. Default to True.
    :param timestamp: Whether to include a time suffix in the format of _HHMMSS. Default to True.
    :param dt_override: Value to override the time stamps. If not provided, defaults to the current datetime.
    :return: Generated suffix with datestamps and timestamps.
    """
    # Add date time suffix if required
    if dt_override is None:
        dt_now = datetime.datetime.now()
    else:
        dt_now = dt_override

    if datestamp:
        suffix += dt_now.strftime("_%Y%m%d")
    if timestamp:
        suffix += dt_now.strftime("_%H%M%S")
    return suffix


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

    :param df: Dataframe.
    :return: A mapping of column names to SQLAlchemy types with size limits.
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


def parse_table_name(tbl_name: str) -> SqlTable:
    """ Splits provided table name into database, schema and table parts.

    :param tbl_name: Name of the table.
    :return: Parsed table name with schema and table parts separated.
    """
    schema = None
    parts = tbl_name.split(".")
    if len(parts) >= 2:
        schema, *table_name_list = parts
        table_name = ".".join(table_name_list)
    elif len(parts) == 2:
        schema, table_name = parts
    else:
        table_name = parts[0]
    return SqlTable(schema=schema, name=table_name)


def geopandas_to_pandas(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """ Converts a geodataframe into an ordinary pandas dataframe.

    :param gdf: Geodataframe.
    :return: Ordinary pandas dataframe.
    """
    geo_cols = gdf.select_dtypes('geometry').columns

    # If applicable, set the only geo column to active column so that srid can be fetched
    if len(geo_cols) == 1:
        gdf.set_geometry(geo_cols[0], inplace=True, drop=True)
    srid = gdf.crs.to_epsg()

    # Add the srid into the dataframe
    gdf["srid"] = srid
    for col in geo_cols:
        gdf[col] = gdf[col].apply(lambda geom: '' if geom is None else geom.wkt)

    return pd.DataFrame(gdf)


def cleanse_dataframe(df: pd.DataFrame, standardise_column_names: bool = True, cleanse_string_columns: bool = True):
    """ Cleanse the input dataframe and perform auto field sizing.

    :param df: Input dataframe.
    :param standardise_column_names: Whether to standardise column names.
    :param cleanse_string_columns: Whether to perform a default cleansing.
    :return: Cleansed dataframe.
    """
    if standardise_column_names:
        df = _cleanse_column_names(df)
    if cleanse_string_columns:
        df = _cleanse_string_columns(df)
    return df


def write_dataframe_to_sql(df: pd.DataFrame, config: SqlWriteConfig, conn_info: SqlConnectionResource):
    """ Write the provided dataframe to SQL table. Returns standard metadata for convenience.

    :param df: The dataframe to ingest.
    :param config: Additional configuration for writing table to SQL.
    :param conn_info: Connection info for the database.
    :return: Asset materialisation metadata for Dagster.
    """
    start = time.time()
    schema = config.sql_table.schema
    table_name = config.sql_table.name

    # Modify various object names per configuration
    if config.to_lower_schema:
        schema = cleanse_obj_name(schema, ignore_dot=True)
    if config.to_lower_table:
        table_name = cleanse_obj_name(table_name)

    if config.cleanse_column_name:
        df = df.rename(columns=cleanse_obj_name)
    if config.auto_field and len(df.index) > 0:
        dtype = auto_field(df)
    else:
        dtype = None

    engine = conn_info.get_engine()
    if config.direct_write:
        df.to_sql(f"{schema}.{table_name}", con=engine, index=False, if_exists="replace", chunksize=config.chunksize)
    else:
        df.to_sql(table_name, schema=schema, con=engine, index=False, if_exists="replace", chunksize=config.chunksize,
                  method=config.method, dtype=dtype)
    end = time.time()

    return {
        "Execution Duration": end - start,
        "dagster/row_count": len(df),
        "dagster/relation_identifier": config.sql_table.get_table_name(),
    }


def copy_table(source_table: SqlTable, target_table: SqlTable, conn_info: SqlConnectionResource):
    """ Move SQL table within the same database. Returns standard metadata dictionary for convenience..

    :param source_table: Source table.
    :param target_table: Target table.
    :param conn_info: Connection info.
    :return: Asset materialisation metadata for Dagster.
    """
    engine = conn_info.get_engine()
    with engine.connect() as conn:
        source_table_clause = source_table.get_table_name()
        target_table_clause = target_table.get_table_name()
        conn.execute(text(f"""DROP TABLE IF EXISTS {target_table_clause}"""))
        res = conn.execute(text(f"""SELECT * INTO {target_table_clause} FROM {source_table_clause}"""))
        conn.commit()
        return {
            "dagster/row_count": res.rowcount,
            "dagster/relation_identifier": target_table.get_table_name(),
        }


class AssetCollection(Collection[AssetSpec | AssetsDefinition]):
    """ Define a collection of assets in the module, with methods to populate it on the fly.

    When this package is developed, `dagster.load_asset_from_module` can only pick up assets defined through the
    `dagster.asset` decorator.
    Any asset defined using factory methods or through `AssetSpec` may not be detected properly.
    This class addresses the limitation by populating a list of assets defined through these means, which can be passed
    to the definitions to ensure they are recognised by dagster.
    """

    def __init__(self):
        self._assets = []

    def add_asset_spec(self, key: str, **kwargs):
        """ Add an asset spec and register it in the asset catalogue.

        :param key: The asset spec key.
        :param kwargs: Additional keyword arguments passed to Dagster.
        :return: Dagster AssetSpec.
        """
        spec = dagster.AssetSpec(key, **kwargs)
        self._assets.append(spec)
        return spec

    def asset(self, compute_fn: Callable | None = None, **kwargs) -> dagster.AssetsDefinition:
        """ Add an asset and register it in the asset catalogue.

        :param compute_fn: The asset spec key.
        :param kwargs: Additional keyword arguments passed to Dagster.
        :return: Dagster Asset.
        """
        asset = dagster.asset(compute_fn, **kwargs)
        self._assets.append(asset)
        return asset

    def register(self, asset: AssetSpec | AssetsDefinition):
        """ A decorator that registers the asset in the local collection. """
        self._assets.append(asset)
        return asset

    def get_assets(self) -> list[AssetSpec | AssetsDefinition]:
        """ Returns the underlying list of assets or asset specs. """
        return self._assets

    def __len__(self):
        return len(self._assets)

    def __iter__(self):
        return iter(self._assets)

    def __contains__(self, x, /):
        return x in self._assets


def sql_to_sql_asset_factory(source_table: SqlTable, target_table: SqlTable):
    """ Creates a simple EL pipeline that copy-loads a database table to another, within the same database.

    :param source_table: Source table in the database.
    :param target_table: Target table in the database.
    :return: Dagster AssetsDefinition.
    """
    def _wrapped_asset_definition(conn_info: SqlConnectionResource):
        metadata = copy_table(source_table, target_table, conn_info)
        return MaterializeResult(
            metadata=metadata,
        )

    return _wrapped_asset_definition
