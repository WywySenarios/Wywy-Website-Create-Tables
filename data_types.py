from typing import Any, Literal, TypeAlias, TypedDict

Datatype: TypeAlias = Literal["int", "integer", "float", "number", "string", "str", "text", "bool", "boolean", "date", "time", "timestamp", "enum", "geodetic point"]
PostgresDatatype: TypeAlias = Literal["integer", "real", "double precision", "text", "boolean", "date", "time", "timestamp", "interval", "enum", "ST_Point"]

class DataColumn(TypedDict):
    name: str
    parser: Datatype
    datatype: Datatype
    defaultValue: Any
    entrytype: str
    invalidInputMessage: str | None
    comments: bool | None
    description: str | None
    unique: bool | None

class DescriptorInfo(TypedDict):
    name: str
    schema: list[DataColumn]

class TableInfo(TypedDict):
    tableName: str
    entrytype: Literal["form", "timer"]
    read: bool
    write: bool
    comments: bool
    schema: list[DataColumn]
    descriptors: list[DescriptorInfo]

class DatabaseInfo(TypedDict):
    dbname: str
    tables: list[TableInfo]

class MainConfig(TypedDict):
    data: list[DatabaseInfo]