import re
from typing import Sequence, Literal, Hashable, Any, Annotated, Union, TypeVar

import dagster as dg
import sqlalchemy
from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema

from . import task

CoercibleToAssetKeyPrefix = str | Sequence[str]


class NoDefault(dg.Model):
    pass


class Destination(dg.Model):
    type: str
    write_options: dg.Model


class SqlTable(dg.Model):
    """ A wrapper of sqlalchemy table. """
    name: str = Field(
        default=None,
        description="Name of table.",
    )
    schema: str | None = Field(
        default=None,
        description="Name of schema. For databases and warehouses with additional schema "
                    "layers, include them all in the schema.",
    )
    schema_quoting: bool | None = Field(
        default=None,
        description="Whether to enforce quoting around the schema name. By default, the quotes are "
                    "automatically generated if necessary."
    )
    name_quoting: bool | None = Field(
        default=None,
        description="Whether to enforce quoting around the table name. By default, the quotes are "
                    "automatically generated if necessary."
    )

    def get_table_name(self, schema_postfix: str = "") -> str:
        """ A quick method to get formatted table names.

        When SQLAlchemy syntax can be used, it should be preferred over generating names manually in this way since it
        has limited capabilities.

        :param schema_postfix: Optional postfix for schema name.
        :return: Parsed SQL table name.
        """
        # Check if the string is lower case, alphanumeric only. Otherwise, use quoting.
        schema_quoting = _is_quoting_enabled(self.schema, self.schema_quoting, ignore_dot=True)
        name_quoting = _is_quoting_enabled(self.name, self.name_quoting, ignore_dot=False)

        # Schema quoting.
        if schema_quoting:
            schema_str = f'"{self.schema}{schema_postfix}"'
        else:
            schema_str = f'{self.schema}{schema_postfix}'

        # Name quoting only.
        if name_quoting:
            name_str = f'"{self.name}"'
        else:
            name_str = f'{self.name}'

        return f'{schema_str}.{name_str}'

    def get_drop_sql(self, checkfirst: bool = True) -> sqlalchemy.sql.text:
        """ A rough ANSI SQL implementation of table dropping.

        :param checkfirst: Whether to check the existence of the table first.
        :return: SQL code for dropping the table.
        """
        table_name = self.get_table_name()
        if checkfirst:
            return sqlalchemy.sql.text(f"""drop table if exists {table_name}""")
        else:
            return sqlalchemy.sql.text(f"""drop table {table_name}""")


class CsvReadOptions(dg.Model):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)
    sep: str | SkipJsonSchema[None] = Field(default=None)
    delimiter: str | SkipJsonSchema[None] = Field(default=None)
    header: int | Sequence[int] | Literal["infer"] | SkipJsonSchema[None] = Field(default=None)
    names: Sequence[Hashable] | SkipJsonSchema[None] = Field(default=None)
    chunksize: int | SkipJsonSchema[None] = Field(default=None)
    encoding: str | SkipJsonSchema[None] = Field(default=None)
    # dtype: DtypeArg | None = Field(default=None)
    # engine: CSVEngine | None = Field(default=None)
    # converters: Mapping[Hashable, Callable] | None = Field(default=None)
    # true_values: list | None = Field(default=None)
    # false_values: list | None = Field(default=None)
    # skipinitialspace: bool = Field(default=None)
    # skiprows: list[int] | int | Callable[[Hashable], bool] | None = Field(default=None)
    # skipfooter: int = Field(default=None)
    # nrows: int | None = Field(default=None)
    # NA and Missing Data Field(default=None)
    # na_values: Hashable | Iterable[Hashable] | Mapping[Hashable, Iterable[Hashable]] | None = Field(default=None)
    # keep_default_na: bool = Field(default=None)
    # na_filter: bool = Field(default=None)
    # verbose: bool | lib.NoDefault = Field(default=None)
    # skip_blank_lines: bool = Field(default=None)
    # parse_dates: bool | Sequence[Hashable] | None = Field(default=None)
    # infer_datetime_format: bool | lib.NoDefault = Field(default=None)
    # keep_date_col: bool | lib.NoDefault = Field(default=None)
    # date_parser: Callable | lib.NoDefault = Field(default=None)
    # date_format: str | dict[Hashable, str] | None = Field(default=None)
    # dayfirst: bool = Field(default=None)
    # cache_dates: bool = Field(default=None)
    # iterator: bool = Field(default=None)
    # compression: CompressionOptions = Field(default=None)
    # thousands: str | None = Field(default=None)
    # decimal: str = Field(default=None)
    # lineterminator: str | None = Field(default=None)
    # quotechar: str = Field(default=None)
    # quoting: int = Field(default=None)
    # doublequote: bool = Field(default=None)
    # escapechar: str | None = Field(default=None)
    # comment: str | None = Field(default=None)
    # encoding_errors: str | None = Field(default=None)
    # dialect: str | csv.Dialect | None = Field(default=None)
    # on_bad_lines: str = Field(default=None)
    # delim_whitespace: bool | lib.NoDefault = Field(default=None)
    # low_memory: bool = Field(default=None)
    # memory_map: bool = Field(default=None)
    # float_precision: Literal["high", "legacy"] | None = Field(default=None)
    # storage_options: StorageOptions | None = Field(default=None)
    # dtype_backend: DtypeBackend | lib.NoDefault = Field(default=None)


class CsvReadOptionsDuckdb(dg.Model):
    backend: Literal["duckdb"] = Field(default="duckdb", exclude=True)
    sep: str | SkipJsonSchema[None] = Field(default=None)
    delimiter: str | SkipJsonSchema[None] = Field(default=None)
    header: int | Sequence[int] | Literal["infer"] | SkipJsonSchema[None] = Field(default=None)
    names: Sequence[Hashable] | SkipJsonSchema[None] = Field(default=None)
    chunksize: int | SkipJsonSchema[None] = Field(default=None)
    encoding: str | SkipJsonSchema[None] = Field(default=None)


CsvReadOptionsType = TypeVar("CsvReadOptionsType", CsvReadOptions, CsvReadOptionsDuckdb)


class CsvSource[CsvReadOptionsType](dg.Model):
    """ A wrapper of a CSV file using the pandas backend. """
    type: Literal["csv"] = "csv"
    path: str = Field(
        description="Path to the workbook.",
    )
    read_options: CsvReadOptionsType = Field(
        default_factory=CsvReadOptions,
        description="Options to pass to pandas backend.",
        discriminator="backend",
    )


class ExcelReadOptions(dg.Model):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)
    header: int | Sequence[int] | SkipJsonSchema[None] = Field(default=None)
    names: Sequence[Hashable] | range | SkipJsonSchema[None] = Field(default=None)
    # index_col: int | str | Sequence[int] | None = ...
    # usecols: int | str | Sequence[int] | Sequence[str] | Callable[[str], bool] | None = ...
    # dtype: DtypeArg | None = ...
    # engine: Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"] | None = ...
    # converters: dict[str, Callable] | dict[int, Callable] | None = ...
    # true_values: Iterable[Hashable] | None = ...
    # false_values: Iterable[Hashable] | None = ...
    # skiprows: Sequence[int] | int | Callable[[int], object] | None = ...
    # nrows: int | None = ...
    # na_values = ...
    # keep_default_na: bool = ...
    # na_filter: bool = ...
    # verbose: bool = ...
    # parse_dates: list | dict | bool = ...
    # date_parser: Callable | lib.NoDefault = ...
    # date_format: dict[Hashable, str] | str | None = ...
    # decimal: str = ...
    # thousands: str | None = ...
    # comment: str | None = ...
    # skipfooter: int = ...
    # storage_options: StorageOptions | None = ...
    # dtype_backend: DtypeBackend | lib.NoDefault = ...
    # engine_kwargs: dict | None = ...


class ExcelSource(dg.Model):
    """ A wrapper of an Excel table. """
    type: Literal["excel"] = Field(default="excel", exclude=True)
    path: str = Field(
        description="Path to the workbook.",
    )
    sheet_name: str | int | None = Field(
        default=None,
        description="Name or index of the worksheet."
    )
    read_options: ExcelReadOptions = Field(
        default_factory=ExcelReadOptions,
        description="Options to pass to pandas backend.",
    )


class ParquetReadOptions(dg.Model):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)


class ParquetReadOptionsDuckdb(dg.Model):
    backend: Literal["duckdb"] = Field(default="duckdb", exclude=True)


ParquetReadOptionsType = TypeVar("ParquetReadOptionsType", ParquetReadOptions, ParquetReadOptionsDuckdb)


class ParquetSource[ParquetReadOptionsType](dg.Model):
    type: Literal["parquet"] = Field(default="parquet", exclude=True)
    path: str = Field(
        description="Path to the workbook.",
    )
    read_options: ParquetReadOptionsType = Field(
        default_factory=ParquetReadOptions,
        description="Options to pass to pandas backend.",
        discriminator="backend",
    )


class JsonReadOptions(dg.Model):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)


class JsonReadOptionsDuckdb(dg.Model):
    backend: Literal["duckdb"] = Field(default="duckdb", exclude=True)


JsonReadOptionsType = TypeVar("JsonReadOptionsType", JsonReadOptions, JsonReadOptionsDuckdb)

class JsonSource(dg.Model):
    type: Literal["json"] = Field(default="json", exclude=True)
    path: str = Field(
        description="Path to the workbook.",
    )
    read_options: JsonReadOptionsType = Field(
        default_factory=JsonReadOptions,
        description="Options to pass to pandas backend.",
        discriminator="backend",
    )


class CsvToSqlConfig(dg.Model):
    backend: Literal["pandas", "sqlalchemy"] = "pandas"


class SqlToPandasConfig(dg.Model):
    pass


def _is_quoting_enabled(qualifier: str, quoting: bool | None, ignore_dot=True):
    if ignore_dot:
        pattern = re.compile(r"^[a-z_][a-z0-9_.]*$")
    else:
        pattern = re.compile(r"^[a-z_][a-z0-9_]*$")

    if quoting is None:
        return pattern.fullmatch(qualifier) is None

    return quoting


class SqlTableReadOptions(dg.Model):
    backend: Literal["pandas", "sqlalchemy"] = "pandas"
    schema: str | SkipJsonSchema[None] = None
    index_col: str | list[str] | SkipJsonSchema[None] = None
    coerce_float: bool | SkipJsonSchema[None] = None
    parse_dates: list[str] | dict[str, str] | SkipJsonSchema[None] = None
    columns: list[str] | SkipJsonSchema[None] = None
    chunksize: int | SkipJsonSchema[None] = None
    # dtype_backend: DtypeBackend | lib.NoDefault = lib.no_default


class SqlWriteOptions(dg.Model):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)
    direct_write: bool = Field(
        default=False,
        description="Write directly to the database and bypass db / schema specifications."
    )
    standardise_db_name: bool = Field(
        default=False,
        description="Whether to standardise database name."
    )
    to_lower_schema: bool = Field(
        default=True,
        description="Whether to standardise schema name."
    )
    to_lower_table: bool = Field(
        default=True,
        description="Whether to standardise table name."
    )
    cleanse_column_name: bool = Field(
        default=True,
        description="Whether to standardise column names."
    )
    auto_field: bool = Field(
        default=True,
        description="Whether to automatically set the smallest SQL text field data type."
    )
    chunksize: int = Field(
        default=200000,
        description="Size of each chunk. A lower number improves memory efficiency while a higher number improves performance.",
    )
    method: Literal["multi"] | SkipJsonSchema[None] = Field(
        default=None,
        description="Method of insertion.",
    )


class SqlDestination(dg.Model):
    type: Literal["sql"] = Field(default="sql", exclude=True)
    sql_table: SqlTable = Field(
        description="The target table to write."
    )
    write_options: SqlWriteOptions = Field(
        default_factory=SqlWriteOptions,
        description="Options for writing.",
    )


class SqlSource(dg.Model):
    type: Literal["sql"] = Field(default="sql", exclude=True)
    sql_table: SqlTable = Field(
        description="The source table to read from."
    )
    read_options: SqlTableReadOptions = Field(
        default_factory=SqlTableReadOptions,
        description="Options for reading.",
    )


class ComputeFnConfig(BaseModel):
    pass


class SqlCopyFactory(ComputeFnConfig):
    compute_fn: Literal["sql_copy"] = "sql_copy"
    source: SqlSource
    destination: SqlDestination


class ReadFileFactory(ComputeFnConfig):
    compute_fn: Literal["csv_to_sql"] = "read_file"
    source: CsvSource | ExcelSource | JsonSource | ParquetSource = Field(discriminator="type")

    def __call__(self, *args, **kwargs):
       pass


FactoryType = Annotated[ReadFileFactory | SqlCopyFactory, Field(discriminator="compute_fn")]


class ModelFile(BaseModel):
    name: str
    description: str
    pipeline: list[FactoryType] = Field(
        description="A list of steps to be performed by the asset."
    )
    kinds: list[str] | None = None
    group_name: str | None = None
    key_prefix: CoercibleToAssetKeyPrefix | None = None
    owners: list[str] | None = None
    tags: dict[str, str] | None = None


class AssetArgs(BaseModel):
    name: str
    compute_fn: str
    key_prefix: CoercibleToAssetKeyPrefix | None = None
    owners: list[str] | None = None
    kinds: list[str] | None = None
    description: str | None = None
    tags: dict[str, str] | None = None
    group_name: str | None = None
    # ins: dict[str, AssetIn] | None = None
    # deps: list[CoercibleToAssetDep] | None = None
    # metadata: ArbitraryMetadataMapping | None = None
    # config_schema: UserConfigSchema | None = None
    # required_resource_keys: AbstractSet[str] | None = None
    # resource_defs: dict[str, object] | None = None
    # io_manager_def: object | None = None
    # io_manager_key: str | None = None
    # dagster_type: DagsterType | None = None
    # partitions_def: PartitionsDefinition | None = None
    # op_tags: Mapping[str, Any] | None = None
    # output_required: bool = True
    # automation_condition: AutomationCondition | None = None
    # backfill_policy: BackfillPolicy | None = None
    # retry_policy: RetryPolicy | None = None
    # code_version: str | None = None
    # key: CoercibleToAssetKey | None = None
    # check_specs: Sequence[AssetCheckSpec] | None = None

    # model_config = ConfigDict(
    #     extra='allow',
    # )


class Project(BaseModel):
    name: str
    version: str
    profile: str = Field(
        description="This setting configures which 'profile' dbt uses for this project."
    )
    model_paths: list[str] = Field(
        description="The paths to the model files."
    )
    macro_paths: list[str] = Field(
        description="The paths to the macro files."
    )
    # models: ModelConfig


class ArchiveTableOpConfig(dg.Model):
    tables: list[SqlTable] = Field(
        description="A list of tables to archive.",
    )
    archive_schema: str | None = Field(
        default=None,
        description="Schema to archive the table. If not specified, the same schema will be used. "
                    "In this case, it is recommended to add prefix or suffix to separate it from production tables.",
    )
    prefix: str = Field(
        default="",
        description="The prefix to prepend to the table names. Defaults to empty string.",
    )
    suffix: str = Field(
        default="",
        description="The suffix to append to the table names, before date and time stamps. Defaults to empty string.",
    )
    datestamp: bool = Field(
        default=True,
        description="Whether to add datestamps in the format of _YYYYMMDD to the table names.",
    )
    timestamp: bool = Field(
        default=True,
        description="Whether to add timestamps in the format of _HHMMSS to the table names.",
    )


class DumpTableToExcel(dg.Model):
    format: Literal["xlsx"] = Field(
        default="xlsx",
    )
    method: Literal["multi"] | None = Field(
        None,
        description="Method of insertion.",
    )
    file_name: str = Field(
        description="Path to the excel file.",
    )
    sql_tables: SqlTable | list[SqlTable] = Field(
        description="The table to read from.",
    )
    chunksize: int = Field(
        200000,
        description="Size of each chunk. A lower number improves memory efficiency while a higher number improves performance.",
    )
    args: dict[str, Any] = Field(
        default_factory=lambda: {},
        description=".",
    )


class DumpTableToCsv(dg.Model):
    format: Literal["csv"] = Field(
        default="csv"
    )
    method: Literal["pandas"] = Field(
        description="Backend tool to run the insertion.",
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
    source: str
    destination: Destination
    backend: str
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

    el_models: list[ELModel]

    # added fields here will define params when instantiated in Python, and yaml schema via Resolvable

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _assets = []

        for model in self.el_models:

            source = resolve_source(dct["source"])
            destination = resolve_destination(dct["destination"])
            backend = dct["backend"]

            asset_key = f"{model.source}_to_{model.destination}".replace(".", "_")

            el_function = el_registry.find(model.source.type, model.destination.type)
            asset = dg.asset(compute_fn=el_function, name=asset_key)

            _assets.append(asset)

        _resources = {
            "conn_info": s3.S3Resource(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
            )
        }

        return dg.Definitions(assets=_assets, resources=_resources)
