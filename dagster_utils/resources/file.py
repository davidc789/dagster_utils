from abc import ABC
from typing import Literal, Sequence, Hashable

import dagster as dg
from pydantic import Field
from pydantic.json_schema import SkipJsonSchema

from dagster_utils.resources.abc import DataSpecResource, SourceReadConfig
from dagster_utils.utils.hashing import HashAlgorithm, file_hash


class FileSpecResource(DataSpecResource, ABC):
    type: Literal["file"] = "file"
    path: str = Field(
        description="Path to the file.",
    )


class ParquetSpec(FileSpecResource):
    type: Literal["parquet"] = Field(default="parquet")
    path: str = Field(
        description="Path to the workbook.",
    )


class JsonSpec(FileSpecResource):
    type: Literal["json"] = Field(default="json")
    path: str = Field(
        description="Path to the workbook.",
    )


class CsvSpec(FileSpecResource):
    """ A wrapper of a CSV file using the pandas backend. """
    type: Literal["csv"] = Field(default="csv")
    path: str = Field(
        description="Path to the workbook.",
    )
    backend: str | Literal["pandas", "duckdb"] = Field()


class ExcelSpec(FileSpecResource):
    """ A wrapper of an Excel table. """
    type: Literal["excel"] = Field(default="excel")
    path: str = Field(
        description="Path to the workbook.",
    )
    sheet_name: str | int | None = Field(
        default=None,
        description="Name or index of the worksheet."
    )


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


class ParquetReadOptions(SourceReadConfig):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)


class ParquetReadOptionsDuckdb(SourceReadConfig):
    backend: Literal["duckdb"] = Field(default="duckdb", exclude=True)


class JsonReadOptions(SourceReadConfig):
    backend: Literal["pandas"] = Field(default="pandas", exclude=True)


class JsonReadOptionsDuckdb(SourceReadConfig):
    backend: Literal["duckdb"] = Field(default="duckdb", exclude=True)


class SqlTableReadOptions(SourceReadConfig):
    backend: Literal["pandas", "sqlalchemy"] = "pandas"
    schema: str | SkipJsonSchema[None] = None
    index_col: str | list[str] | SkipJsonSchema[None] = None
    coerce_float: bool | SkipJsonSchema[None] = None
    parse_dates: list[str] | dict[str, str] | SkipJsonSchema[None] = None
    columns: list[str] | SkipJsonSchema[None] = None
    chunksize: int | SkipJsonSchema[None] = None
    # dtype_backend: DtypeBackend | lib.NoDefault = lib.no_default


def file_observer(file_path: str, method: Literal["content", "metadata"] = "content",
                  hash_algorithm: HashAlgorithm = "md5") -> str:
    return dg.DataVersion(file_hash(file_path, method, hash_algorithm))
