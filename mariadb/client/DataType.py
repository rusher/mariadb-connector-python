import datetime
import json
from enum import Enum

from mariadb.client import DataTypeMap
from mariadb.client.ReadableByteBuf import ReadableByteBuf


def default_text_parse(buf: ReadableByteBuf, col):
    if col.ext_type_name == 'json':
        return json.loads(buf.read_string_length_encoded())
    if col.charset == 63:
        return buf.read_length_buffer()

    value = buf.read_string_length_encoded()
    if col.flags & 2048 > 0:
        # SET
        if not value:
            return None
        else:
            return value.split(',')
    return value

def read_json_text(buf: ReadableByteBuf, col=None):
    return json.loads(buf.read_string_length_encoded())

def read_buffer_text(buf: ReadableByteBuf, col=None):
    return buf.read_length_buffer()

def read_string_text(buf: ReadableByteBuf, col=None):
    return buf.read_string_length_encoded()

def read_float_length_encoded(buf: ReadableByteBuf):
    return buf.read_float_length_encoded()


def read_int_length_encoded(buf: ReadableByteBuf):
    return buf.read_int_length_encoded()


def read_datetime_length_encoded(buf: ReadableByteBuf):
    return buf.read_datetime_length_encoded()

def read_date_length_encoded(buf: ReadableByteBuf):
    return buf.read_date_length_encoded()

def read_time_length_encoded(buf: ReadableByteBuf):
    return buf.read_time_length_encoded()

def read_decimal(buf: ReadableByteBuf, col):
    length = buf.read_length()
    return float(buf.read_ascii(length))


def read_tiny(buf: ReadableByteBuf, col):
    return buf.read_byte() if col.is_signed() else buf.read_unsigned_byte()


def read_small(buf: ReadableByteBuf, col):
    return buf.read_short() if col.is_signed() else buf.read_unsigned_short()


def read_int(buf: ReadableByteBuf, col):
    return buf.read_int() if col.is_signed() else buf.read_unsigned_int()


def read_bigint(buf: ReadableByteBuf, col):
    return buf.read_long() if col.is_signed() else buf.read_unsigned_long()

def read_float(buf: ReadableByteBuf, col):
    return buf.read_float()

def read_double(buf: ReadableByteBuf, col):
    return buf.read_double()


def read_datetime(buf: ReadableByteBuf, col):
    length = buf.read_length()
    if length == 0:
        return None

    year = buf.read_unsigned_short()
    month = buf.read_byte()
    day_of_month = buf.read_byte()
    hour, minutes, seconds, microseconds = 0, 0, 0, 0

    if length > 4:
        hour = buf.read_byte()
        minutes = buf.read_byte()
        seconds = buf.read_byte()

        if length > 7:
            microseconds = buf.read_unsigned_int()
    return datetime.datetime(year, month, day_of_month, hour, minutes, seconds, microseconds)


def read_date(buf: ReadableByteBuf, col):
    length = buf.read_length()
    if length == 0:
        return None

    year = buf.read_unsigned_short()
    month = buf.read_byte()
    day_of_month = buf.read_byte()

    return datetime.date(year, month, day_of_month)


def read_time(buf: ReadableByteBuf, col):
    length = buf.read_length()
    if length == 0:
        return None

    buf.skip(3) # negate + days

    hour = buf.read_byte()
    minutes = buf.read_byte()
    seconds = buf.read_byte()
    microseconds = 0
    if length > 8:
        microseconds = buf.read_unsigned_int()

    return datetime.time(hour, minutes, seconds, microseconds)


class DataType(bytes, Enum):
    OLDDECIMAL = (0, read_float_length_encoded, read_decimal)
    TINYINT = (1, read_int_length_encoded, read_tiny)
    SMALLINT = (2, read_int_length_encoded, read_small)
    INTEGER = (3, read_int_length_encoded, read_int)
    FLOAT = (4, read_float_length_encoded, read_float)
    DOUBLE = (5, read_float_length_encoded, read_double)
    NULL = (6, lambda b, c: None, lambda b, c: None)
    TIMESTAMP = (7, read_datetime_length_encoded, read_datetime)
    BIGINT = (8, read_int_length_encoded, read_bigint)
    MEDIUMINT = (9, read_int_length_encoded, read_int)
    DATE = (10, read_date_length_encoded, read_date)
    TIME = (11, read_time_length_encoded, read_time)
    DATETIME = (12, read_datetime_length_encoded, read_datetime)
    YEAR = (13, read_int_length_encoded, read_small)
    NEWDATE = (14, read_date_length_encoded, read_date)
    VARCHAR = (15, read_string_text, read_string_text)
    BIT = (16, read_buffer_text, read_buffer_text)
    JSON = (245, read_json_text, lambda c, b: b.loads(b.read_string_length_encoded()))
    DECIMAL = (246, read_float_length_encoded, read_decimal)
    ENUM = (247, read_string_text, read_string_text)
    SET = (248, read_string_text, read_string_text)
    TINYBLOB = (249, read_buffer_text, read_buffer_text)
    MEDIUMBLOB = (250, read_buffer_text, read_buffer_text)
    LONGBLOB = (251, read_buffer_text, read_buffer_text)
    BLOB = (252, read_buffer_text, read_buffer_text)
    VARSTRING = (253, read_string_text, read_string_text)
    STRING = (254, read_string_text, read_string_text)
    GEOMETRY = (255, read_buffer_text, read_buffer_text)

    def __new__(cls, value, parse_text, parse_binary):
        obj = bytes.__new__(cls, [value])
        obj._value_ = value
        obj.parse_text = parse_text
        obj.parse_binary = parse_binary
        return obj

    def get(self):
        return self.value

    @staticmethod
    def of(type_value: int):
        return DataTypeMap.type_map[type_value]
