import json
import struct

from mariadb.client import DataTypeMap
from mariadb.client.DataType import DataType
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.util.constant import ColumnFlags

INT_PARSER = struct.Struct('<i')

class Column:
    __slots__ = ('data_type', 'saved', 'charset', 'length', 'decimals', 'flags', 'ext_type_name')

    def __init__(self, saved: bytearray, length: int, data_type: DataType, charset: int,
                 decimals: int, flags: int, ext_type_name: str):
        self.data_type = data_type
        self.saved = saved
        self.charset = charset
        self.length = length
        self.decimals = decimals
        self.flags = flags
        self.ext_type_name = ext_type_name

    @staticmethod
    def decode(buf: ReadableByteBuf, extended_info: bool):

        saved = buf.save_buf()
        pos = len(saved) - 12

        ext_type_name = None
        # if extended_info:
        #     # fast skipping extended info (usually not set)
        #     if buf.read_byte() != 0:
        #         # revert position, because has extended info.
        #         buf.pos = buf.pos - 1
        #         sub_packet = buf.read_length_buffer()
        #         while sub_packet.readable_bytes() > 0:
        #             if sub_packet.read_byte() == 0:
        #                 ext_type_name = sub_packet.read_ascii(sub_packet.read_length())
        #             else:
        #                 # skip data
        #                 sub_packet.skip(sub_packet.read_length())

        # buf.skip()  # skip length always 0x0c
        charset = ((saved[pos] & 0xff) + ((saved[pos + 1] & 0xff) << 8))
        length, = INT_PARSER.unpack_from(saved, pos + 2)
        data_type = DataTypeMap.type_map[saved[pos + 6]]
        flags = (saved[pos + 7] & 0xff + (saved[pos + 8] & 0xff) << 8)
        decimals = saved[pos + 9]

        # str_buf = buf.buf[0:string_pos[4]]
        return Column(saved, length, data_type, charset, decimals, flags, ext_type_name)

    def is_signed(self) -> bool:
        return (self.flags & ColumnFlags.UNSIGNED) == 0

    def get_display_size(self) -> int:
        if self.data_type == DataType.VARCHAR or self.data_type == DataType.JSON or self.data_type == DataType.ENUM or self.data_type == DataType.SET or self.data_type == DataType.VARSTRING or self.data_type == DataType.STRING:
            # TODO
            return 0
        return self.length

    def is_primary_key(self) -> bool:
        return (self.flags & ColumnFlags.PRIMARY_KEY) > 0

    def is_autoincrement(self) -> bool:
        return (self.flags & ColumnFlags.AUTO_INCREMENT) > 0

    def has_default(self) -> bool:
        return (self.flags & ColumnFlags.NO_DEFAULT_VALUE_FLAG) == 0

    # doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
    # like string), but have the binary flag
    def is_binary(self) -> bool:
        return self.charset == 63

    def get_precision(self) -> int:
        if self.data_type == DataType.OLDDECIMAL:
            if self.is_signed():
                return self.length - (2 if self.decimals > 0 else 1)
            else:
                return self.length - (1 if self.decimals > 0 else 0)
        if self.data_type == DataType.VARCHAR or self.data_type == DataType.JSON or self.data_type == DataType.ENUM or self.data_type == DataType.SET or self.data_type == DataType.VARSTRING or self.data_type == DataType.STRING:
            # Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
            return 0
        else:
            return self.length

    def parser(self, binary: bool):
        if binary:
            if self.data_type == DataType.TINYINT:
                return lambda buf: buf.read_byte() if self.is_signed() else lambda b: b.read_unsigned_byte()
            if self.data_type == DataType.SMALLINT or self.data_type == DataType.YEAR:
                return lambda buf: buf.read_short() if self.is_signed() else lambda b: b.read_unsigned_short()
            if self.data_type == DataType.INTEGER or self.data_type == DataType.MEDIUMINT:
                return lambda buf: buf.read_int() if self.is_signed() else lambda b: b.read_unsigned_int()
            if self.data_type == DataType.BIGINT:
                return lambda buf: buf.read_long() if self.is_signed() else lambda b: b.read_unsigned_long()
            if self.data_type == DataType.FLOAT:
                return lambda buf: buf.read_float()
            if self.data_type == DataType.DOUBLE:
                return lambda buf: buf.read_double()
            if self.data_type == DataType.TIMESTAMP or self.data_type == DataType.TIMESTAMP:
                return lambda buf: buf.read_datetime()
            if self.data_type == DataType.DATE or self.data_type == DataType.NEWDATE:
                return lambda buf: buf.read_date()
            if self.data_type == DataType.TIME:
                return lambda buf: buf.read_time()

        else:
            if self.data_type == DataType.TINYINT or self.data_type == DataType.SMALLINT or self.data_type == DataType.YEAR or self.data_type == DataType.MEDIUMINT or self.data_type == DataType.INTEGER or self.data_type == DataType.BIGINT:
                return lambda buf: buf.read_int_length_encoded()
            if self.data_type == DataType.TIMESTAMP or self.data_type == DataType.DATETIME:
                return lambda buf: buf.read_datetime_length_encoded()
            if self.data_type == DataType.DATE or self.data_type == DataType.NEWDATE:
                return lambda buf: buf.read_date_length_encoded()
            if self.data_type == DataType.TIME:
                return lambda buf: buf.read_time_length_encoded()
            if self.data_type == DataType.FLOAT or self.data_type == DataType.DOUBLE:
                return lambda buf: buf.read_float_length_encoded()

        if self.data_type == DataType.OLDDECIMAL or self.data_type == DataType.DECIMAL:
            return lambda buf: buf.read_float_length_encoded()
        if self.ext_type_name == 'json' or self.data_type == DataType.JSON:
            return lambda buf: json.loads(buf.read_string_length_encoded())
        if self.charset == 63:
            return lambda buf: buf.read_length_buffer()
        if self.flags & 2048 > 0:
            return lambda buf: buf.read_set_length_encoded()
        return lambda buf: buf.read_string_length_encoded()

    def loaded(self) -> bool:
        return True
