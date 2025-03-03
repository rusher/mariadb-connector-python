import copy
import logging
import struct

from mariadb.util import ExceptionFactory, LoggerHelper

SMALL_BUFFER_SIZE = 8192
QUOTE = b'\''
DBL_QUOTE = b'"'
ZERO_BYTE = b'\0'
BACKSLASH = b'\\'
SMALL_BUFFER_SIZE = 8192
MEDIUM_BUFFER_SIZE = 128 * 1024
LARGE_BUFFER_SIZE = 1024 * 1024
MAX_PACKET_LENGTH = 0x00ffffff + 4

SHORT_PARSER = struct.Struct('<h')
INT_PARSER = struct.Struct('<i')
LONG_PARSER = struct.Struct('<q')
DOUBLE_PARSER = struct.Struct('<d')

class PacketWriter:
    logger = logging.getLogger(__name__)

    __slots__ = ('socket', 'initial_buf', 'buf', 'max_query_size_to_log', 'cmd_length',
                 'sequence', 'pos', 'max_packet_length', 'max_allowed_packet', 'permit_trace',
                 'server_thread_log', 'mark', 'buf_contain_data_after_mark')

    def __init__(self, sock, max_query_size_to_log, sequence):
        self.socket = sock
        self.initial_buf = bytearray(SMALL_BUFFER_SIZE)
        self.buf = memoryview(self.initial_buf)
        self.max_query_size_to_log = max_query_size_to_log
        self.cmd_length = 0
        self.sequence = sequence
        self.pos = 4
        self.max_packet_length = MAX_PACKET_LENGTH
        self.max_allowed_packet = 1024 * 1024 * 1024
        self.permit_trace = True
        self.server_thread_log = ''
        self.mark = -1
        self.buf_contain_data_after_mark = False

    def get_cmd_length(self):
        return self.cmd_length

    def write_byte(self, value):
        if self.pos >= len(self.buf):
            if self.pos >= self.max_packet_length and not self.buf_contain_data_after_mark:
                # buf is more than a Packet, must flush_buf()
                self.write_socket(False)
            else:
                self.grow_buffer(1)
        self.buf[self.pos] = value
        self.pos += 1

    def write_short(self, value):
        if 2 >= len(self.buf) - self.pos:
            # not enough space remaining
            self.write_bytes(SHORT_PARSER.pack(value), 2)
            return
        SHORT_PARSER.pack_into(self.buf, self.pos, value)
        self.pos += 2

    def write_int(self, value):
        if 4 >= len(self.buf) - self.pos:
            # not enough space remaining
            self.write_bytes(INT_PARSER.pack(value), 4)
            return
        INT_PARSER.pack_into(self.buf, self.pos, value)
        self.pos += 4

    def write_long(self, value):
        if 8 > len(self.buf) - self.pos:
            # not enough space remaining
            self.write_bytes(LONG_PARSER.pack(value), 8)
            return
        LONG_PARSER.pack_into(self.buf, self.pos, value)
        self.pos += 8

    def write_double(self, value):
        if 8 >= len(self.buf) - self.pos:
            # not enough space remaining
            self.write_bytes(DOUBLE_PARSER.pack(value), 8)
            return
        DOUBLE_PARSER.pack_into(self.buf, self.pos, value)
        self.pos += 8

    def write_bytes_at_pos(self, b, pos):
        self.buf[pos:pos + len(b)] = b

    def write_bytes(self, b, length):
        if length > len(self.buf) - self.pos:
            if len(self.buf) != self.max_packet_length:
                self.grow_buffer(length)
            # max buf size
            if length > len(self.buf) - self.pos:
                if self.mark != -1:
                    self.grow_buffer(length)
                if self.mark != -1:
                    self.flush_buffer_stop_at_mark()
            if length > len(self.buf) - self.pos:
                # not enough space in buf, will stream :
                # fill buf and flush until all data are snd
                remaining_len = length
                off = 0
                while True:
                    len_to_fill_buf = min(self.max_packet_length - self.pos, remaining_len)
                    self.buf[self.pos:self.pos + len_to_fill_buf] = [copy.deepcopy(x) for x in
                                                                     b[off, off + len_to_fill_buf]]
                    remaining_len -= len_to_fill_buf
                    off += len_to_fill_buf
                    self.pos += len_to_fill_buf
                    if remaining_len > 0:
                        self.write_socket(False)
                    else:
                        break
                return
        self.buf[self.pos:(self.pos + length)] = b
        self.pos += length

    def write_length(self, length) -> None:
        if length < 251:
            self.write_byte(length)
            return
        if length < 65536:
            if self.pos + 3 >= len(self.buf):
                # not enough space remaining
                b = bytearray(3)
                b[0] = 0xfc
                b[1] = length & 0xFF
                b[2] = length >> 8
                self.write_bytes(b, 3)
                return
            self.buf[self.pos] = 0xfc
            self.buf[self.pos + 1] = length & 0xFF
            self.buf[self.pos + 2] = length >> 8
            self.pos += 3
            return

        if length < 16777216:
            if self.pos + 4 >= len(self.buf):
                # not enough space remaining
                b = bytearray(3)
                b[0] = 0xfd
                b[1] = length & 0xFF
                b[2] = length >> 8
                b[3] = length >> 16
                self.write_bytes(b, 4)
                return
            self.buf[self.pos] = 0xfd
            self.buf[self.pos + 1] = length & 0xFF
            self.buf[self.pos + 2] = length >> 8
            self.buf[self.pos + 3] = length >> 16
            self.pos += 4
            return

        if self.pos + 9 >= len(self.buf):
            # not enough space remaining
            b = bytearray(3)
            b[0] = 0xfe
            b[1] = length & 0xFF
            b[2] = length >> 8
            b[3] = length >> 16
            b[4] = length >> 24
            b[5] = length >> 32
            b[6] = length >> 40
            b[7] = length >> 48
            b[8] = length >> 56
            self.write_bytes(b, 9)
            return
        self.buf[self.pos] = 0xfe
        self.buf[self.pos + 1] = length & 0xFF
        self.buf[self.pos + 2] = length >> 8
        self.buf[self.pos + 3] = length >> 16
        self.buf[self.pos + 4] = length >> 24
        self.buf[self.pos + 5] = length >> 32
        self.buf[self.pos + 6] = length >> 40
        self.buf[self.pos + 7] = length >> 48
        self.buf[self.pos + 8] = length >> 56
        self.pos += 9

    def write_ascii(self, val):
        b = val.encode('ascii')
        self.write_bytes(b, len(b))

    def write_string(self, val):
        b = val.encode()
        self.write_bytes(b, len(b))

    def write_string_escaped(self, val, no_backslash_escapes):
        b = val.encode()
        self.write_bytes_escaped(b, len(b), no_backslash_escapes)

    def write_bytes_escaped(self, val, length, no_backslash_escapes):
        if self.pos + length * 2 > len(self.buf):
            if len(self.buf) != self.max_packet_length:
                self.grow_buffer(length * 2)

            # data may be bigger than buf.
            # must flush buf when full (and reset position to 0)
            if self.pos + length * 2 > len(self.buf):
                if self.mark != -1:
                    self.grow_buffer(length * 2)
                    if self.mark != -1:
                        self.flush_buffer_stop_at_mark()
                else:
                    # not enough space in buf, will fill buf
                    if no_backslash_escapes:
                        for i in range(0, length):
                            if QUOTE == val[i]:
                                self.buf[self.pos] = QUOTE
                                self.pos += 1
                                if len(self.buf) <= self.pos:
                                    self.write_socket(False)

                            self.buf[self.pos] = val[i]
                            self.pos += 1
                            if len(self.buf) <= self.pos:
                                self.write_socket(False)
                    else:
                        for i in range(0, length):
                            if val[i] == QUOTE or val[i] == BACKSLASH or val[i] == DBL_QUOTE or val[i] == ZERO_BYTE:
                                self.buf[self.pos] = '\\'
                                self.pos += 1
                                if len(self.buf) <= self.pos:
                                    self.write_socket(False)
                            self.buf[self.pos] = val[i]
                            self.pos += 1
                            if len(self.buf) <= self.pos:
                                self.write_socket(False)
                    return

        # sure to have enough place filling buf directly
        if no_backslash_escapes:
            for i in range(0, length):
                if QUOTE == val[i]:
                    self.buf[self.pos] = QUOTE
                    self.pos += 1
                self.buf[self.pos] = val[i]
                self.pos += 1
        else:
            idx = 0
            for i in range(idx, length):
                if val[i] == QUOTE or val[i] == BACKSLASH or val[i] == DBL_QUOTE or val[i] == ZERO_BYTE:
                    if i > idx:
                        self.buf[self.pos: self.pos + (i - idx)] = val[idx:i]
                        self.pos += i - idx
                        idx = i
                    self.buf[self.pos] = '\\'
                    self.pos += 1
            if idx < length:
                self.buf[self.pos: self.pos + (length - idx)] = val[idx:length]
                self.pos += length - idx

    def grow_buffer(self, length):
        buf_length = len(self.buf)
        new_capacity = 0
        if buf_length == SMALL_BUFFER_SIZE:
            if length + self.pos <= MEDIUM_BUFFER_SIZE:
                new_capacity = MEDIUM_BUFFER_SIZE
            elif length + self.pos <= LARGE_BUFFER_SIZE:
                new_capacity = LARGE_BUFFER_SIZE
            else:
                new_capacity = self.max_packet_length
        elif buf_length == MEDIUM_BUFFER_SIZE:
            if length + self.pos <= LARGE_BUFFER_SIZE:
                new_capacity = LARGE_BUFFER_SIZE
            else:
                new_capacity = self.max_packet_length
        elif self.buf_contain_data_after_mark:
            # want to add some information to buf without having the command Header
            # must grow buf until having all the query
            new_capacity = max(length + self.pos, self.max_packet_length)
        else:
            new_capacity = self.max_packet_length

        if length + self.pos > new_capacity:
            if self.mark != -1:
                # buf is > 16M with mark.
                # flush until mark, reset pos at beginning
                self.flush_buffer_stop_at_mark()

                if length + self.pos <= buf_length:
                    return

                # need to keep all data, buf can grow more than maxPacketLength
                # grow buf if needed
                if buf_length == self.max_packet_length:
                    return
                if length + self.pos > new_capacity:
                    new_capacity = min(self.max_packet_length, length + self.pos)

        new_buf = bytearray(new_capacity)
        new_buf_view = memoryview(new_buf)
        new_buf_view[0:self.pos] = self.buf[0:self.pos]
        self.initial_buf = new_buf
        self.buf = new_buf_view

    def flush(self):
        self.write_socket(True)

        # if buf is big, and last query doesn't use at least half of it, resize buf to default value
        if len(self.buf) > SMALL_BUFFER_SIZE and self.cmd_length * 2 < len(self.buf):
            buf = bytearray(SMALL_BUFFER_SIZE)
        self.pos = 4
        self.cmd_length = 0
        self.mark = -1

    def check_max_allowed_length(self, length):
        if self.cmd_length + length >= self.max_packet_length:
            # launch exception only if no packet has been sent.
            raise ExceptionFactory.MaxAllowedPacketException(
                "query size {} is >= to max_allowed_packet {}".format(str(self.cmd_length + length),
                                                                      self.max_allowed_packet))

    def throw_max_allowed_length(self, length):
        return self.cmd_length + length >= self.max_allowed_packet

    def set_max_allowed_packet(self, max_allowed_packet):
        self.max_allowed_packet = max_allowed_packet
        self.max_packet_length = min(MAX_PACKET_LENGTH, self.max_allowed_packet + 4)

    def set_server_thread_id(self, server_thread_id, host_address):
        is_master = host_address.primary if host_address is not None else None
        self.server_thread_log = "conn={} ({})".format(server_thread_id, is_master)

    def mark_pos(self):
        self.mark = self.pos

    def is_marked(self):
        return self.mark != -1

    def has_flushed(self):
        return self.sequence[0] == 0xff

    def flush_buffer_stop_at_mark(self):
        end = self.pos
        self.pos = self.mark
        self.write_socket(True)
        self.init_packet()
        self.buf[self.pos:self.pos + end - self.mark] = [copy.deepcopy(x) for x in self.buf[self.mark:end]]
        self.pos += end - self.mark
        self.mark = -1
        self.buf_contain_data_after_mark = True

    def buf_is_data_after_mark(self):
        return self.buf_contain_data_after_mark

    def reset_mark(self):
        self.pos = self.mark
        self.mark = -1

        if self.buf_contain_data_after_mark:
            data = bytearray(self.pos - 4)
            data[0:self.pos - 4] = self.buf[4:self.pos]
            self.init_packet()
            self.buf_contain_data_after_mark = False
            return data
        return None

    def init_packet(self):
        self.sequence[0] = 0xff
        self.sequence[1] = 0xff
        self.pos = 4
        self.cmd_length = 0

    def write_socket(self, command_end):
        length = self.pos - 4
        if length > 0:
            self.sequence[0] = self.sequence[0] + 1 & 0xff
            INT_PARSER.pack_into(self.buf, 0, length)
            self.buf[3] = self.sequence[0]

            self.check_max_allowed_length(length)

            if PacketWriter.logger.isEnabledFor(logging.DEBUG):
                if self.permit_trace:
                    trace = LoggerHelper.hex(self.buf, 0, self.pos, self.max_query_size_to_log)
                    PacketWriter.logger.debug("send: " + self.server_thread_log + "\n" + trace)
                else:
                    PacketWriter.logger.debug(
                        "send: content length={} {} com=<hidden>".format(str(length), self.server_thread_log))
            self.socket.sendall(self.buf[0:self.pos])
            self.cmd_length += length

            # if last com fill the max size, must send an empty com to indicate command end.
            if command_end and self.pos == self.max_packet_length:
                self.write_empty_packet()
            self.pos = 4

    def write_empty_packet(self):
        self.sequence[0] = self.sequence[0] + 1 & 0xff
        INT_PARSER.pack_into(self.buf, 0, 0)
        self.buf[3] = self.sequence[0]

        if PacketWriter.logger.isEnabledFor(logging.DEBUG):
            trace = LoggerHelper.hex(self.buf, 0, 4)
            PacketWriter.logger.debug("send: " + self.server_thread_log + "\n" + trace)

        self.socket.sendall(self.buf[0:4])
        self.cmd_length = 0

    def close(self):
        self.socket.close()
