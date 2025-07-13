import datetime
from pathlib import Path
from collections.abc import Collection
from types import ModuleType
from typing import Callable

import dagster
import pandas as pd
import geopandas as gpd
from dagster import AssetSpec, AssetsDefinition

from dagster_utils.pipeline import SqlTable

PathLike = str | Path


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


def parse_table_name(tbl_name: str) -> SqlTable:
    """ Splits provided table name into database, schema and table parts.

    :param tbl_name: name of the table.
    :return: Database, schema and table name parts.
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
    return SqlTable(
        schema=schema,
        name=table_name,
    )


def geopandas_to_pandas(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """ Perform spatial transformation on gdf.

    :param gdf: geodataframe.
    :return: Cleansed geodataframe.
    """
    geo_cols = gdf.select_dtypes('geometry').columns

    # If applicable set the only geo column to active column so that srid can be fetched
    if len(geo_cols) == 1:
        gdf.set_geometry(geo_cols[0], inplace=True, drop=True)
    srid = gdf.crs.to_epsg()

    # Add the srid into the dataframe
    gdf["srid"] = srid
    for col in geo_cols:
        gdf[col] = gdf[col].apply(lambda geom: '' if geom is None else geom.wkt)

    return pd.DataFrame(gdf)


class AssetCollection(Collection[AssetSpec | AssetsDefinition]):
    """ Define a collection of assets in the module. """
    # Whether to load all assets into the module for dynamic reference.
    _module: ModuleType | None

    def __init__(self, module: ModuleType = None):
        self._assets = []
        self._module = module

    def asset_spec(self, key: str, **kwargs):
        spec = dagster.AssetSpec(key, **kwargs)
        self.register(spec)
        return spec

    def asset(self, compute_fn: Callable | None = None, **kwargs):
        asset = dagster.asset(compute_fn, **kwargs)
        self.register(asset)
        return asset

    def register(self, asset: AssetSpec | AssetsDefinition):
        """ A wrapper that registers the asset in the local asset collection and returns it back. """
        self._assets.append(asset)
        if self._module is not None:
            setattr(self._module, "_".join(asset.key.parts), asset)
        return asset

    def add_assets(self, assets: list[AssetSpec | AssetsDefinition] | AssetSpec | AssetsDefinition):
        """ Add assets in bulk.

        :param assets: A list of assets to add.
        """
        if not isinstance(assets, Collection):
            assets = [assets]

        for asset in assets:
            self.register(asset)

    def __len__(self):
        return len(self._assets)

    def __iter__(self):
        return iter(self._assets)

    def __contains__(self, x, /):
        return x in self._assets
