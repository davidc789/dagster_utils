import warnings
from typing import Literal, Any, ClassVar, Sequence, Hashable

import dagster as dg
from dagster import Config, SourceAsset
from dagster._core.definitions.decorators.source_asset_decorator import _ObservableSourceAsset
from dagster._core.definitions.scoped_resources_builder import Resources
from pydantic import BaseModel, Field
from pydantic.json_schema import SkipJsonSchema

from .pipeline import SqlTable, Destination
from .registry import Registry
from .serialise import file_hash, HashAlgorithm
from .tasks import Task


class DestinationWriteConfig(BaseModel):
    pass


class SourceReadConfig(BaseModel):
    pass


class Source(BaseModel):
    """ Base class for all sources.

    All sources should inherit from this class to ensure they are registered with the package registry.
    """
    name: str
    type: str
    backend: str
    key_prefix: str | list[str]
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

    registry: ClassVar[Registry["type[Source]"]] = Registry()

    def __init_subclass__(cls, **kw: Any) -> None:
        if cls.__dict__.get("registry") is not None:
            warnings.warn(
                f"The registry attribute on {cls.__name__} is used for internal purposes by the package."
                "It is typically not recommended to override this attribute in subclasses.",
                UserWarning,
            )

        cls.registry.register(cls.__name__, cls)
        super().__init_subclass__(**kw)


class FileSource(Source):
    type: Literal["file"] = "file"
    path: str = Field(
        description="Path to the file.",
    )
    config: dict[str, Any] | None = None


class CsvReadOptions(SourceReadConfig):
    _backend: str
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


class CsvReadOptionsPandas(CsvReadOptions):
    _backend: Literal["pandas"] = Field(default="pandas", exclude=True)
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


class CsvReadOptionsDuckdb(CsvReadOptions):
    _backend: Literal["duckdb"] = Field(default="duckdb", exclude=True)
    sep: str | SkipJsonSchema[None] = Field(default=None)
    delimiter: str | SkipJsonSchema[None] = Field(default=None)
    header: int | Sequence[int] | Literal["infer"] | SkipJsonSchema[None] = Field(default=None)
    names: Sequence[Hashable] | SkipJsonSchema[None] = Field(default=None)
    chunksize: int | SkipJsonSchema[None] = Field(default=None)
    encoding: str | SkipJsonSchema[None] = Field(default=None)


class CsvSource(FileSource):
    """ A wrapper of a CSV file using the pandas backend. """
    type: Literal["csv"] = "csv"
    path: str = Field(
        description="Path to the workbook.",
    )
    backend: str | Literal["pandas", "duckdb"] = Field()
    read_options: CsvReadOptions = Field(
        default_factory=CsvReadOptions,
        description="Options to pass to pandas backend.",
        discriminator="backend",
    )


class ExcelReadOptions(SourceReadConfig):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)
    header: int | Sequence[int] | SkipJsonSchema[None] = Field(default=None)
    names: Sequence[Hashable] | range | SkipJsonSchema[None] = Field(default=None)
    # index_col: int | str | Sequence[int] | None = ...
    # usecols: int | str | Sequence[int] | Sequence[str] | Callable[[str], bool] | None = ...
    # dtype: DtypeArg | None = ...
    # engine: Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calxamine"] | None = ...
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


class ExcelSource(FileSource):
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


class ParquetReadOptions(Config):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)


class ParquetReadOptionsDuckdb(Config):
    backend: Literal["duckdb"] = Field(default="duckdb", exclude=True)


class ParquetSource(FileSource):
    type: Literal["parquet"] = Field(default="parquet", exclude=True)
    path: str = Field(
        description="Path to the workbook.",
    )
    read_options: ParquetReadOptions = Field(
        default_factory=ParquetReadOptions,
        description="Options to pass to pandas backend.",
        discriminator="backend",
    )


class JsonReadOptions(Config):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)


class JsonReadOptionsDuckdb(Config):
    backend: Literal["duckdb"] = Field(default="duckdb", exclude=True)


class JsonSource(FileSource):
    type: Literal["json"] = Field(default="json", exclude=True)
    path: str = Field(
        description="Path to the workbook.",
    )
    read_options: JsonReadOptionsDuckdb = Field(
        default_factory=JsonReadOptions,
        description="Options to pass to pandas backend.",
        discriminator="backend",
    )


class SqlTableReadOptions(Config):
    backend: Literal["pandas", "sqlalchemy"] = "pandas"
    schema: str | SkipJsonSchema[None] = None
    index_col: str | list[str] | SkipJsonSchema[None] = None
    coerce_float: bool | SkipJsonSchema[None] = None
    parse_dates: list[str] | dict[str, str] | SkipJsonSchema[None] = None
    columns: list[str] | SkipJsonSchema[None] = None
    chunksize: int | SkipJsonSchema[None] = None
    # dtype_backend: DtypeBackend | lib.NoDefault = lib.no_default


class SqlSource(Config):
    type: Literal["sql"] = Field(default="sql", exclude=True)
    sql_table: SqlTable = Field(
        description="The source table to read from."
    )
    read_options: SqlTableReadOptions = Field(
        default_factory=SqlTableReadOptions,
        description="Options for reading.",
    )


class ElModel(BaseModel):
    name: str
    description: str | SkipJsonSchema[None] = None
    resources: list[str]
    source: str
    destination: Destination
    backend: str
    read_config: SourceReadConfig
    write_config: DestinationWriteConfig

    #     - name: jaffle_shop_customers
    #     description: ''
    #     resources:
    #     - conn_info
    #
    #
    # source:
    #   jaffle_shop
    # destination:
    #   type: sql
    #   sql_table:
    #     schema: public
    #     name: jaffle_shop
    #   conn_info: conn_info
    #
    # Optional specification of how the data should be moved, along with any configurations for the backend.
    # backend: pandas
    pass


def file_observer(file_path: str, method: Literal["content", "metadata"] = "content",
                  hash_algorithm: HashAlgorithm = "md5") -> str:
    return dg.DataVersion(file_hash(file_path, method, hash_algorithm))


def parse_el_model(dct: dict[str, Any]) -> dg.Asset:
    source = resolve_source(dct["source"])
    destination = resolve_destination(dct["destination"])
    backend = dct["backend"]

    # description: ''
    # source: jaffle_shop
    # destination:
    #   type: sql
    #   sql_table:
    #     schema: public
    #     name: jaffle_shop
    #     conn_info: conn_info
