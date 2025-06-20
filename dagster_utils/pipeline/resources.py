import re
from typing import Sequence, Literal, Hashable, Any, Annotated

import sqlalchemy
from dagster import Config
from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema

CoercibleToAssetKeyPrefix = str | Sequence[str]


class NoDefault(Config):
    pass


class Source(Config):
    read_options: Config


class Destination(Config):
    write_options: Config


class SqlTable(Config):
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


class CsvReadOptions(Config):
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


class CsvSource(Config):
    """ A wrapper of a CSV file using the pandas backend. """
    type: Literal["csv"] = "csv"
    path: str = Field(
        description="Path to the workbook.",
    )
    read_options: CsvReadOptions = Field(
        default_factory=CsvReadOptions,
        description="Options to pass to pandas backend.",
    )


class ExcelReadOptions(Config):
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


class ExcelSource(Config):
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


class CsvToSqlConfig(Config):
    backend: Literal["pandas", "sqlalchemy"] = "pandas"


class SqlToPandasConfig(Config):
    pass


def _is_quoting_enabled(qualifier: str, quoting: bool | None, ignore_dot=True):
    if ignore_dot:
        pattern = re.compile(r"^[a-z_][a-z0-9_.]*$")
    else:
        pattern = re.compile(r"^[a-z_][a-z0-9_]*$")

    if quoting is None:
        return pattern.fullmatch(qualifier) is None

    return quoting


class SqlTableReadOptions(Config):
    backend: Literal["pandas", "sqlalchemy"] = "pandas"
    schema: str | SkipJsonSchema[None] = None
    index_col: str | list[str] | SkipJsonSchema[None] = None
    coerce_float: bool | SkipJsonSchema[None] = None
    parse_dates: list[str] | dict[str, str] | SkipJsonSchema[None] = None
    columns: list[str] | SkipJsonSchema[None] = None
    chunksize: int | SkipJsonSchema[None] = None
    # dtype_backend: DtypeBackend | lib.NoDefault = lib.no_default


class SqlWriteOptions(Config):
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


class SqlDestination(Config):
    type: Literal["sql"] = Field(default="sql", exclude=True)
    sql_table: SqlTable = Field(
        description="The target table to write."
    )
    write_options: SqlWriteOptions = Field(
        default_factory=SqlWriteOptions,
        description="Options for writing.",
    )


class SqlSource(Config):
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


class ExtractLoadConfig(ComputeFnConfig):
    source: Source
    destination: Destination


class SqlCopyFactory(ExtractLoadConfig):
    compute_fn: Literal["sql_copy"] = "sql_copy"
    source: SqlSource
    destination: SqlDestination


class CsvToSqlFactory(ExtractLoadConfig):
    compute_fn: Literal["csv_to_sql"] = "csv_to_sql"
    source: CsvSource | ExcelSource = Field(discriminator="type")
    destination: SqlDestination


FactoryType = Annotated[CsvToSqlFactory | SqlCopyFactory, Field(discriminator="compute_fn")]


class ModelFile(BaseModel):
    name: str
    description: str
    # This needs to be particular pipeline Configs
    pipeline: list[FactoryType]
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
