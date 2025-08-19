import re
from typing import Union, Any, Literal, Annotated, ClassVar

import pandas as pd
import sqlalchemy
from dagster import Config, ConfigurableResource, InitResourceContext
from pydantic import Field, ConfigDict, PrivateAttr, model_validator, BaseModel
from sqlalchemy import create_engine, Engine, MetaData
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


class SqlTable(Config):
    """ A wrapper of sqlalchemy table in Dagster Config. """
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
        description="Whether to generate quotes around the schema name. "
                    "By default, the quotes are automatically generated only if necessary."
    )
    name_quoting: bool | None = Field(
        default=None,
        description="Whether to generate quotes around the table name. "
                    "By default, the quotes are automatically generated only if necessary."
    )

    def get_table_name(self) -> str:
        """ A quick method to get formatted table names.

        When SQLAlchemy syntax can be used, it should be preferred to generating names manually in this way since it
        has limited capabilities.

        :return: Parsed SQL table name.
        """
        # Check if the string is lower case, alphanumeric only. Otherwise, use quoting.
        schema_quoting = _is_quoting_enabled(self.schema, self.schema_quoting, ignore_dot=True)
        name_quoting = _is_quoting_enabled(self.name, self.name_quoting, ignore_dot=False)

        # Schema quoting
        if schema_quoting:
            schema_str = f'"{self.schema}"'
        else:
            schema_str = f'{self.schema}'

        # Name quoting only
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


class MetaDataResource(ConfigurableResource):
    schema: str | None = Field(
        default=None,
        title="Schema",
        description="The default schema to use.",
    )
    quote_schema: bool | None = Field(
        default=None,
        title="Qute Schema",
        description="Whether to quote schema and table names for object in the metadata.",
    )
    info: dict | None = Field(
        default=None,
        title="Info",
        description="Additional information passed onto the table API.",
    )
    # naming_convention

    _metadata: MetaData | None = PrivateAttr(None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._metadata = MetaData(schema=self.schema, quote_schema=self.quote_schema, info=self.info)

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        super().teardown_after_execution(context)

        if self._metadata is not None:
            self._metadata = None

    def get_metadata(self) -> MetaData:
        if self._metadata is None:
            raise ValueError("MetaData not initialised properly.")

        return self._metadata


class ConnectionComponents(Config):
    """ A dagster config wrapper of parameters to use to create a SQLAlchemy engine URL. Ported from prefect-sqlalchemy.

    :param driver: The driver name to use.
    :param database: The name of the database to use.
    :param username: The username used to authenticate.
    :param password: The password used to authenticate.
    :param host: The host address of the database.
    :param port: The port to connect to the database.
    :param query: A dictionary of string keys to string values to be passed to dialect and/or DBAPI upon connect.
    """
    driver: str = Field(
        default=..., description="The driver name to use."
    )
    database: str = Field(
        default=..., description="The name of the database to use."
    )
    username: str | None = Field(
        default=None, description="The username used to authenticate."
    )
    password: str | None = Field(
        default=None, description="The password used to authenticate."
    )
    host: str | None = Field(
        default=None, description="The host address of the database."
    )
    port: int | None = Field(
        default=None, description="The port to connect to the database."
    )
    query: dict[str, str] | None = Field(
        default=None,
        description=(
            "A dictionary of string keys to string values to be passed to the dialect "
            "and/or the DBAPI upon connect. To specify non-string parameters to a "
            "Python DBAPI directly, use connect_args."
        ),
    )

    def create_url(self) -> URL:
        """ Create a fully formed connection URL.

        :return: The SQLAlchemy engine URL.
        """
        url_params = dict(
            drivername=self.driver,
            username=self.username,
            password=self.password,
            database=self.database,
            host=self.host,
            port=self.port,
            query=self.query,
        )
        return URL.create(
            **{
                url_key: url_param
                for url_key, url_param in url_params.items()
                if url_param is not None
            }
        )


class SqlConnectionResource(ConfigurableResource):
    """ Specifies a SQL connection resource. """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    url: str | None = Field(
        default=None, deprecated="SQLAlchemy URL to create the engine"
    )
    connection_info: ConnectionComponents | None = Field(
        default=..., description="SQLAlchemy engine URL connection components",
    )
    create_engine_args: dict[str, Any] | None = Field(
        default_factory=lambda: {},
        title="Additional Engine Arguments",
        description=(
            "The options which will be passed directly to the sqlalchemy's create_engine()"
            "function as additional keyword arguments."
        ),
    )
    connect_args: dict[str, Any] | None = Field(
        default=None,
        title="Additional Connection Arguments",
        description=(
            "The options which will be passed directly to the DBAPI's connect() "
            "method as additional keyword arguments."
        ),
    )
    fetch_size: int = Field(
        default=1, description="The number of rows to fetch at a time."
    )

    _engine: AsyncEngine | Engine | None = PrivateAttr(None)
    _driver_is_async: bool | None = PrivateAttr(None)

    @model_validator(mode="after")
    def validate_url_completeness(self):
        if self.connection_info is None and self.url is None:
            raise ValueError('one of connection_info and url must have a value')
        return self

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """ Initializes the engine.

        :param context: Init context.
        """
        super().setup_for_execution(context)

        if self.connection_info is not None:
            rendered_url = self.connection_info.create_url()
        else:
            # make rendered url from string
            rendered_url = make_url(str(self.url))

        self._driver_is_async = (rendered_url.drivername in [x.default for x in AsyncDriver.model_fields.values()])
        engine_kwargs = dict(
            url=rendered_url,
            connect_args=self.connect_args or {},
            **self.create_engine_args,
        )
        if self._driver_is_async:
            # no need to await here
            self._engine = create_async_engine(**engine_kwargs)
        else:
            self._engine = create_engine(**engine_kwargs)

        context.log.info("Created a new engine.")

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """ Closes sync connections and its cursors.

        :param context: Init context.
        """
        super().teardown_after_execution(context)

        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            context.log.info("Disposed the engine.")

    def get_engine(self) -> Union[Engine, AsyncEngine]:
        """ Returns an authenticated engine that can be used to query from databases.

        Ported from prefect-sqlalchemy implementation. If an existing engine exists, return that one.

        :return: The authenticated SQLAlchemy Engine / AsyncEngine.
        """
        if self._engine is None:
            raise ValueError("Engine not initialised properly.")

        return self._engine


class ArchiveTableOpConfig(Config):
    """ Configuration for archiving a table. """
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
    """ Configuration for dumping a table to Excel. """
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
    """ Configuration for dumping a table to csv. """
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


class DateOffsetConfig(Config):
    """ Configuration for specifying date offsets. """
    _description_1: ClassVar[str] = "Parameters that add to the offset (like Timedelta)."
    _description_2: ClassVar[str] = "Parameters that replace the offset value."

    n: int = Field(
        default=None,
        description="The number of time periods the offset represents. If specified without a temporal pattern, defaults to n days.",
    )
    normalize: bool = Field(
        default=None,
        description="Whether to round the result of a DateOffset addition down to the previous midnight.",
    )

    years: float = Field(
        default=None,
        description=_description_1,
    )
    months: float = Field(
        default=None,
        description=_description_1,
    )
    weeks: float = Field(
        default=None,
        description=_description_1,
    )
    days: float = Field(
        default=None,
        description=_description_1,
    )
    hours: float = Field(
        default=None,
        description=_description_1,
    )
    minutes: float = Field(
        default=None,
        description=_description_1,
    )
    seconds: float = Field(
        default=None,
        description=_description_1,
    )
    milliseconds: float = Field(
        default=None,
        description=_description_1,
    )
    microseconds: float = Field(
        default=None,
        description=_description_1,
    )
    nanoseconds: float = Field(
        default=None,
        description=_description_1,
    )

    year: float = Field(
        default=None,
        description = _description_2
    )
    month: float = Field(
        default=None,
        description = _description_2
    )
    day: float = Field(
        default=None,
        description = _description_2
    )
    weekday: Literal[0, 1, 2, 3, 4, 5, 6] = Field(
        default=None,
        description=_description_2 + " A specific integer for the day of the week, 0 is Monday and 6 is Sunday."
    )
    hour: float = Field(
        default=None,
        description = _description_2
    )
    minute: float = Field(
        default=None,
        description = _description_2
    )
    second: float = Field(
        default=None,
        description = _description_2
    )
    microsecond: float = Field(
        default=None,
        description = _description_2
    )
    nanosecond: float = Field(
        default=None,
        description = _description_2
    )

    def get_dateoffset(self) -> pd.DateOffset:
        return pd.DateOffset(**{k: v for k, v in self.model_dump().items() if v is not None})


class PrepareDevOpConfig(Config):
    tables: list[SqlTable] = Field(
        description="A list of table mappings.",
    )
    schema_suffix: str = Field(
        default="_dev",
        description="Schema suffix for the dev tables.",
    )
    name_suffix: str = Field(
        default="",
        description="Name suffix for the dev tables.",
    )


class KeepAwakeConfig(Config):
    action_interval: float = Field(
        default=60,
        description="Interval in seconds to perform an action.",
    )
    run_duration: float = Field(
        default=None,
        description="Length of time to keep running. No run-time limit is set by default.",
    )


class SqlWriteConfig(Config):
    """ Config for writing pandas dataframe to SQL. """
    sql_table: SqlTable = Field(
        description="The target table to write."
    )
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
    method: Literal["multi"] | None = Field(
        default=None,
        description="Method of insertion.",
    )


class _AsyncDriver(BaseModel):
    """ Known dialects with their corresponding async drivers. Ported from prefect-sqlalchemy. """
    POSTGRESQL_ASYNCPG: str = "postgresql+asyncpg"
    SQLITE_AIOSQLITE: str = "sqlite+aiosqlite"
    MYSQL_ASYNCMY: str = "mysql+asyncmy"
    MYSQL_AIOMYSQL: str = "mysql+aiomysql"


class _SyncDriver(BaseModel):
    """ Known dialects with their corresponding sync drivers. Ported from prefect-sqlalchemy. """
    POSTGRESQL_PSYCOPG2: str = "postgresql+psycopg2"
    POSTGRESQL_PG8000: str = "postgresql+pg8000"
    POSTGRESQL_PSYCOPG2CFFI: str = "postgresql+psycopg2cffi"
    POSTGRESQL_PYPOSTGRESQL: str = "postgresql+pypostgresql"
    POSTGRESQL_PYGRESQL: str = "postgresql+pygresql"
    MYSQL_MYSQLDB: str = "mysql+mysqldb"
    MYSQL_PYMYSQL: str = "mysql+pymysql"
    MYSQL_MYSQLCONNECTOR: str = "mysql+mysqlconnector"
    MYSQL_CYMYSQL: str = "mysql+cymysql"
    MYSQL_OURSQL: str = "mysql+oursql"
    MYSQL_PYODBC: str = "mysql+pyodbc"
    SQLITE_PYSQLITE: str = "sqlite+pysqlite"
    SQLITE_PYSQLCIPHER: str = "sqlite+pysqlcipher"
    ORACLE_CX_ORACLE: str = "oracle+cx_oracle"
    MSSQL_PYODBC: str = "mssql+pyodbc"
    MSSQL_MXODBC: str = "mssql+mxodbc"
    MSSQL_PYMSSQL: str = "mssql+pymssql"


def _is_quoting_enabled(qualifier: str, quoting: bool | None, ignore_dot=True):
    if ignore_dot:
        pattern = re.compile(r"^[a-z_][a-z0-9_.]*$")
    else:
        pattern = re.compile(r"^[a-z_][a-z0-9_]*$")

    if quoting is None:
        return pattern.fullmatch(qualifier) is None

    return quoting


AsyncDriver = _AsyncDriver()
SyncDriver = _SyncDriver()
