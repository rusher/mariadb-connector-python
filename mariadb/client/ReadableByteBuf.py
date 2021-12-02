import json
import struct
from datetime import date, datetime, time
from struct import unpack

BYTE_PARSER = struct.Struct('<b')
SHORT_PARSER = struct.Struct('<h')
SHORT_UNSIGNED_PARSER = struct.Struct('<H')
INT_PARSER = struct.Struct('<i')
INT_UNSIGNED_PARSER = struct.Struct('<I')
LONG_PARSER = struct.Struct('<q')
LONG_UNSIGNED_PARSER = struct.Struct('<Q')


class ReadableByteBuf:
    __slots__ = ('pos', 'buf', 'limit', 'view')

    def __init__(self, buf, pos, limit):
        self.pos = pos
        self.buf = buf
        self.view = memoryview(buf)
        self.limit = limit

    def readable_bytes(self):
        return self.limit - self.pos

    def reset(self, buf, pos, limit):
        self.pos = pos
        self.buf = buf
        self.view = memoryview(buf)
        self.limit = limit

    def skip_one(self):
        self.pos += 1

    def skip(self, length=1):
        self.pos += length

    def read_blob(self, length):
        self.pos += length
        return self.buf[self.pos - length:length]

    def get_byte(self, index=None):
        return self.buf[self.pos] if index is None else self.buf[index]

    def get_unsigned_byte(self):
        return self.buf[self.pos] & 0xff

    def read_length(self):
        length = self.buf[self.pos]
        self.pos += 1

        if length < 0xfb:
            return length & 0xff
        if length == 0xfb:
            return None
        if length == 0xfc:
            return self.read_unsigned_short()
        if length == 0xfd:
            return self.read_unsigned_medium()
        return self.read_long()

    def read_length_not_null(self):
        length = self.buf[self.pos]
        self.pos += 1
        if length < 0xfb:
            return length
        if length == 0xfc:
            return self.read_unsigned_short()
        if length == 0xfd:
            return self.read_unsigned_medium()
        return self.read_long()

    def read_int_length_encoded(self) -> int:
        length = self.buf[self.pos]
        if length < 0xfb:
            self.pos += length + 1
        else:
            self.pos += 1
            if length == 0xfb:
                return None
            if length == 0xfc:
                length = self.read_unsigned_short()
            elif length == 0xfd:
                length = self.read_unsigned_medium()
            else:
                length = self.read_long()
            self.pos += length
        return int(self.view[self.pos - length: self.pos])

    def read_float_length_encoded(self) -> float:
        length = self.buf[self.pos]
        if length < 0xfb:
            self.pos += length + 1
        else:
            self.pos += 1
            if length == 0xfb:
                return None
            if length == 0xfc:
                length = self.read_unsigned_short()
            elif length == 0xfd:
                length = self.read_unsigned_medium()
            else:
                length = self.read_long()
            self.pos += length
        return float(self.view[self.pos - length: self.pos])

    def read_date_length_encoded(self) -> date:
        length = self.read_length()
        if length is None:
            return None

        res = []
        value = 0
        init_pos = self.pos
        self.pos += length
        while init_pos < self.pos:
            char = self.buf[init_pos]
            init_pos += 1
            if char == 45:
                # minus separator
                res.append(value)
                value = 0
            else:
                value = value * 10 + char - 48
        res.append(value)

        # handle zero-date as null
        if res[0] == 0 and res[1] == 0 and res[2] == 0:
            return None
        return date(res[0], res[1], res[2])

    def read_datetime_length_encoded(self) -> datetime:
        length = self.read_length()
        if length is None:
            return None
        val = self.read_ascii(length)
        if val.startswith('0000-00-00 00:00:00'):
            return None
        return datetime.fromisoformat(val)

    def read_time_length_encoded(self) -> time:
        length = self.read_length()
        if length is None:
            return None
        val = self.read_ascii(length)
        if val.startswith('0000-00-00 00:00:00'):
            return None
        return time.fromisoformat(val)

    def read_string_length_encoded(self):
        length = self.buf[self.pos]
        if length < 0xfb:
            self.pos += length + 1
        else:
            self.pos += 1
            if length == 0xfb:
                return None
            if length == 0xfc:
                length = self.read_unsigned_short()
            elif length == 0xfd:
                length = self.read_unsigned_medium()
            else:
                length = self.read_long()
            self.pos += length
        return str(self.view[self.pos - length: self.pos], 'utf-8')

    def read_json_length_encoded(self):
        return json.loads(self.read_string_length_encoded())

    def read_set_length_encoded(self):
        length = self.read_length()
        if length is None:
            return None
        self.pos += length
        return str(self.view[self.pos - length: self.pos], 'utf-8').split(',')

    def read_byte(self):
        self.pos += 1
        return BYTE_PARSER.unpack_from(self.buf, self.pos - 1)[0]

    def read_unsigned_byte(self):
        self.pos += 1
        return self.buf[self.pos - 1]

    def read_short(self) -> int:
        self.pos += 2
        return SHORT_PARSER.unpack_from(self.buf, self.pos - 2)[0]

    def read_unsigned_short(self) -> int:
        self.pos += 2
        return SHORT_UNSIGNED_PARSER.unpack_from(self.buf, self.pos - 2)[0]

    def read_int(self) -> int:
        self.pos += 4
        return INT_PARSER.unpack_from(self.buf, self.pos - 4)[0]

    def read_unsigned_int(self):
        self.pos += 4
        return INT_UNSIGNED_PARSER.unpack_from(self.buf, self.pos - 4)[0]

    def read_long(self):
        self.pos += 8
        return LONG_PARSER.unpack_from(self.buf, self.pos - 8)[0]

    def read_unsigned_long(self):
        self.pos += 8
        return LONG_UNSIGNED_PARSER.unpack_from(self.buf, self.pos - 8)[0]

    def read_bytes(self, dest):
        length = len(dest)
        self.pos += length
        dest[0:length] = self.buf[self.pos - length: self.pos]

    def read_bytes_null_end(self):
        cnt = 0
        while self.readable_bytes() > 0 and self.buf[self.pos] != 0:
            cnt += 1
        dst = bytearray(cnt)
        dst[0:cnt] = self.buf[self.pos: self.pos + cnt]
        self.pos += cnt
        return dst

    def read_buffer(self, length):
        self.pos += length
        return self.view[self.pos - length: self.pos]

    def save_buf(self):
        return self.buf[self.pos: self.limit]

    def read_length_buffer(self):
        length = self.read_length_not_null()
        tmp = bytearray(length)
        self.read_bytes(tmp)
        return ReadableByteBuf(tmp, 0, length)

    def read_string(self, length):
        self.pos += length
        return str(self.view[self.pos - length: self.pos], 'utf-8')

    def read_ascii(self, length):
        self.pos += length
        return str(self.view[self.pos - length: self.pos], 'ascii')

    def read_string_null_end(self):
        cnt = 0
        while self.readable_bytes() > cnt and self.buf[self.pos + cnt] != 0:
            cnt += 1
        dst = str(self.view[self.pos: self.pos + cnt], 'utf-8')
        self.pos += cnt + 1
        return dst

    def read_string_eof(self):
        dst = str(self.view[self.pos: self.limit], 'utf-8')
        self.pos = self.limit
        return dst

    def read_float(self):
        self.pos += 4
        return unpack("<f", self.buf[self.pos - 4: self.pos])[0]

    def read_double(self):
        self.pos += 8
        return unpack("<d", self.buf[self.pos - 8: self.pos])[0]

    def read_double_be(self):
        self.pos += 8
        return unpack(">d", self.buf[self.pos - 8: self.pos])[0]

    def read_datetime(self):
        length = self.read_length()
        if length == 0:
            return None
        year = self.read_unsigned_short()
        month = self.read_unsigned_byte()
        day_of_month = self.read_unsigned_byte()
        hour, minutes, seconds, microseconds = 0, 0, 0, 0
        if length > 4:
            hour = self.read_unsigned_byte()
            minutes = self.read_unsigned_byte()
            seconds = self.read_unsigned_byte()
            if length > 7:
                microseconds = self.read_unsigned_int()
        return datetime(year, month, day_of_month, hour, minutes, seconds, microseconds)

    def read_date(self):
        length = self.read_length()
        if length == 0:
            return None
        year = self.read_unsigned_short()
        month = self.read_unsigned_byte()
        day_of_month = self.read_unsigned_byte()
        return date(year, month, day_of_month)

    def read_time(self):
        length = self.read_length()
        if length == 0:
            return None
        self.pos += 3  # negate + days
        hour = self.read_unsigned_byte()
        minutes = self.read_unsigned_byte()
        seconds = self.read_unsigned_byte()
        microseconds = 0
        if length > 8:
            microseconds = self.read_unsigned_int()
        return time(hour, minutes, seconds, microseconds)
