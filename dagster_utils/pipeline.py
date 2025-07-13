import re
from typing import Literal, Any, Annotated, Union, ClassVar, Callable

import sqlalchemy
import dagster as dg
from dagster import Config, CoercibleToAssetKeyPrefix
from dagster._core.definitions.asset_dep import CoercibleToAssetDep
from pydantic import BaseModel, Field
from pydantic.json_schema import SkipJsonSchema
from sqlalchemy import Engine, MetaData, Table

from .registry import Registry
from .sources import Source, SqlSource


class Destination(BaseModel, discriminator="type"):
    type: str
    write_options: Config
    _dagster_dec_registry: ClassVar[Registry] = Registry()


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

    def get_table_name(self, conn: Engine, schema_postfix: str = "") -> str:
        """ A quick method to get formatted table names.

        When SQLAlchemy syntax can be used, it should be preferred over generating names manually in this way since it
        has limited capabilities.

        :param schema_postfix: Optional postfix for schema name.
        :return: Parsed SQL table name.
        """
        metadata = MetaData()
        metadata.reflect(bind=conn, schema=self.schema, only=[self.name])
        if len(metadata.tables) == 0:
            table = Table(self.name, metadata, schema=self.schema)

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


class ComputeFnConfig(BaseModel):
    pass


class SqlCopyFactory(ComputeFnConfig):
    name: Literal["sql_copy"] = "sql_copy"
    source: SqlSource
    destination: SqlDestination


class ReadCsvFactory(ComputeFnConfig):
    name: Literal["csv_to_sql"] = "read_csv"

    def __call__(self, *args, **kwargs):
       pass


FactoryType = Annotated[ReadCsvFactory | SqlCopyFactory, Field(discriminator="name")]


class OpModel(BaseModel):
    name: str | None = None,
    description: str | None = None,
    # ins: Mapping[str, In] | None = None,
    # out: Out | Mapping[str, Out] | None = None,
    # config_schema: UserConfigSchema | None = None,
    # required_resource_keys: AbstractSet[str] | None = None,
    # tags: Mapping[str, Any] | None = None,
    version: str | None = None,
    # retry_policy: RetryPolicy | None = None
    code_version: str | None = None
    pool: str | None = None
    compute_fn: Callable = None


class AssetModel(BaseModel):
    name: str
    description: str
    resources: list[dict[str, Any]] = Field(
        default_factory=list,
        description="A list of resources required by the asset."
    )
    sources: Source = Field(discriminator="type")
    deps: list[CoercibleToAssetDep] | None = None
    destination: Destination = Field(discriminator="type")
    pipeline: list[FactoryType] = Field(
        description="A list of steps to be performed by the asset."
    )
    kinds: list[str] | None = None
    group_name: str | None = None
    key_prefix: dg.CoercibleToAssetKeyPrefix | None = None
    owners: list[str] | None = None
    tags: dict[str, str] | None = None
    # ins: dict[str, AssetIn] | None = None
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


dg.op()

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


class ArchiveTableOpConfig(Config):
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


class DumpTableToExcel(Config):
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


class DumpTableToCsv(Config):
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


class DumpTableOpConfig(Config):
    tables: list[Annotated[Union[DumpTableToCsv, DumpTableToExcel], Field(discriminator="format")]] = Field(
        description="A list of tables to dump",
    )
