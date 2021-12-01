from enum import Enum

from mariadb.client import DataTypeMap


class DataType(bytes, Enum):
    OLDDECIMAL = 0
    TINYINT = 1
    SMALLINT = 2
    INTEGER = 3
    FLOAT = 4
    DOUBLE = 5
    NULL = 6
    TIMESTAMP = 7
    BIGINT = 8
    MEDIUMINT = 9
    DATE = 10
    TIME = 11
    DATETIME = 12
    YEAR = 13
    NEWDATE = 14
    VARCHAR = 15
    BIT = 16
    JSON = 245
    DECIMAL = 246
    ENUM = 247
    SET = 248
    TINYBLOB = 249
    MEDIUMBLOB = 250
    LONGBLOB = 251
    BLOB = 252
    VARSTRING = 253
    STRING = 254
    GEOMETRY = 255

    @staticmethod
    def of(type_value: int):
        return DataTypeMap.type_map[type_value]
