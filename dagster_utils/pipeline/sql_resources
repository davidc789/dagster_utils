from typing import Union, Any, Literal, ClassVar

import pandas as pd
from dagster import Config, ConfigurableResource, InitResourceContext
from pydantic import Field, ConfigDict, PrivateAttr, model_validator, BaseModel
from sqlalchemy import create_engine, Engine, MetaData
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from pipeline import SqlTable


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
    model_config = ConfigDict(arbitrary_types_allowed=True)

    url: str | None = Field(
        default=None,
        description="SQLAlchemy URL to directly create the engine.",
        deprecated="Passing url directly does not support advanced customisations. Please use ConnectionComponents instead."
    )
    connection_info: ConnectionComponents | None = Field(
        default=...,
        description="SQLAlchemy engine URL connection components",
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

        :param context: init context.
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

        :param context: init context.
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


class DateOffsetConfig(Config):
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


class LookForwardConfig(Config):
    shortfall_table: SqlTable = Field(
        description="SQL code for generating the shortfall table.",
    )
    unpaid_table: SqlTable = Field(
        description="SQL code for generating the unpaid leave table.",
    )
    look_forward_offset: DateOffsetConfig = Field(
        description="Look-forward offset.",
    )
    calculator_name: str = Field(
        description="Name of the remediation calculator to display in the output.",
    )
    hour_tol: float = Field(
        default=0.001,
        description="Numeric tolerance for tiny hours",
    )


class PrepareDevOpConfig(Config):
    selection_string: str = Field(
        description="The dagster asset selection string to target in development.",
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


class GlobalConfigResource(ConfigurableResource):
    target: str = Field(
        description="Target profile name, similar to the target variable used by dbt.",
    )
    path: str = Field(
        description="Path to code environment.",
    )
    data_path: str = Field(
        description="Master path to where all data files are stored.",
    )
    download_path: str = Field(
        default=r"C:\Users\L196049\Downloads",
        description="Default path for ad-hoc downloads.",
    )
    schema_postfix: str = Field(
        default="",
        description="Postfix for database schemas to separate the profile with other ones.",
    )
    dev_scope: list[str] = Field(
        default=None,
        description="List of data models to use for development.",
    )
    ci_scope: list[str] = Field(
        default=None,
        description="List of data models to use for validation.",
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


AsyncDriver = _AsyncDriver()
SyncDriver = _SyncDriver()
