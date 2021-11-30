import array

from mariadb.client import DataTypeMap
from mariadb.client.DataType import DataType
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.util.constant import ColumnFlags


class Column:

    __slots__ = ('data_type', 'saved', 'charset', 'length', 'decimals', 'flags', 'ext_type_name')

    def __init__(self, saved: ReadableByteBuf, length: int, data_type: DataType, charset: int,
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

        saved = buf.readablebuffer()
        saved.pos = saved.limit - 12

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

        #buf.skip()  # skip length always 0x0c
        charset = saved.read_short()
        length = saved.read_int()
        data_type = DataTypeMap.type_map[saved.read_unsigned_byte()]
        flags = saved.read_unsigned_short()
        decimals = saved.read_byte()

        # str_buf = buf.buf[0:string_pos[4]]
        return Column(saved, length, data_type, charset, decimals, flags, ext_type_name)

    # @staticmethod
    # def create(name: str, data_type: DataType):
    #     name_bytes = name.encode('utf-8')
    #     arr = bytearray(9 + 2 * len(name_bytes))
    #     arr[0] = 3
    #     arr[1] = ord('D')
    #     arr[2] = ord('E')
    #     arr[3] = ord('F')
    #
    #     string_pos = array.array('i')
    #     string_pos[0] = 4  # schema pos
    #     string_pos[1] = 5  # table alias pos
    #     string_pos[2] = 6  # table pos
    #
    #     # lenenc_str     name
    #     # lenenc_str     org_name
    #     pos = 7
    #     for i in range(0, 2):
    #         string_pos[i + 3] = pos
    #         arr[pos] = len(name_bytes)
    #         pos += 1
    #         arr[pos:len(name_bytes)] = name_bytes
    #         pos += len(name_bytes)
    #
    #     # Sensible predefined length - since we're dealing with I_S here, most char fields are 64 char long
    #     if data_type == DataType.VARCHAR or data_type == DataType.VARSTRING:
    #         length = 64 * 3  # 3 bytes per UTF8 char
    #     elif data_type == DataType.SMALLINT:
    #         length = 5
    #     elif data_type == DataType.NULL:
    #         length = 0
    #     else:
    #         length = 1
    #
    #     return Column(ReadableByteBuf(None, arr, len(arr)), length, data_type, tuple(string_pos), 33, 0,
    #                   ColumnFlags.PRIMARY_KEY, None)

    # def get_schema(self):
    #     b = ReadableByteBuf(None, self.buf, self.string_pos[0], len(self.buf))
    #     return b.read_string(b.read_length_not_null())
    #
    # def get_table_alias(self):
    #     b = ReadableByteBuf(None, self.buf, self.string_pos[1], len(self.buf))
    #     return b.read_string(b.read_length_not_null())
    #
    # def get_table(self):
    #     b = ReadableByteBuf(None, self.buf, self.string_pos[2], len(self.buf))
    #     return b.read_string(b.read_length_not_null())
    #
    # def get_column_alias(self):
    #     b = ReadableByteBuf(None, self.buf, self.string_pos[3], len(self.buf))
    #     return b.read_string(b.read_length_not_null())
    #
    # def get_column_name(self):
    #     b = ReadableByteBuf(None, self.buf, self.string_pos[4], len(self.buf))
    #     return b.read_string(b.read_length_not_null())

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
