import logging
import struct

from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.util import LoggerHelper

MAX_PACKET_SIZE = 0xffffff
PARSER = struct.Struct("<HBB")

class PacketReader:
    logger = logging.getLogger(__name__)

    __slots__ = ('stream', 'max_query_size_to_log', 'sequence', 'server_thread_log', 'readable')

    def __init__(self, stream, conf, sequence):
        self.stream = stream
        self.max_query_size_to_log = conf.get('max_query_size_to_log')
        self.sequence = sequence
        self.server_thread_log = ""
        self.readable = ReadableByteBuf(bytearray(), 0, 0)

    def get_packet_from_socket(self) -> ReadableByteBuf:
        # ***************************************************
        # Read 4 byte header
        # ***************************************************
        header, pos_header, end_header = self.stream.read(4)
        first_2b, last_b, self.sequence[0] = PARSER.unpack_from(header, pos_header)
        last_packet_length = first_2b + (last_b << 16)

        # ***************************************************
        # Read content
        # ***************************************************
        raw, pos, end = self.stream.read(last_packet_length)

        if PacketReader.logger.isEnabledFor(logging.DEBUG):
            b = bytearray(4)
            b[0] = first_2b[0]
            b[1] = first_2b[1]
            b[2] = last_b
            b[3] = self.sequence[0]
            trace = LoggerHelper.hex_header(b, raw, pos, end - pos, self.max_query_size_to_log)
            PacketReader.logger.debug("read: " + self.server_thread_log + "\n" + trace)

        if last_packet_length < MAX_PACKET_SIZE:
            self.readable.reset(raw, pos, end)
            return self.readable

        # ***************************************************
        # In case content length is big, content will be separate in many 16Mb packets
        # ***************************************************
        while True:
            header, pos_header, end_header = self.stream.read(4)
            first_2b, last_b, self.sequence[0] = PARSER.unpack_from(header, pos_header)
            last_packet_length = first_2b + (last_b << 16)

            current_len = len(raw)
            new_raw = bytearray(current_len + last_packet_length)
            new_raw[0:current_len] = raw
            raw = new_raw

            # ***************************************************
            # Read content
            # ***************************************************
            tmp_raw, pos, end = self.stream.read(last_packet_length)
            raw[current_len:current_len] = tmp_raw[pos:end]

            if PacketReader.logger.isEnabledFor(logging.DEBUG):
                b = bytearray(4)
                b[0] = first_2b[0]
                b[1] = first_2b[1]
                b[2] = last_b
                b[3] = self.sequence[0]
                trace = LoggerHelper.hex_header(b, raw, 0, last_packet_length, self.max_query_size_to_log)
                PacketReader.logger.debug("read: " + self.server_thread_log + "\n" + trace)

            if last_packet_length < MAX_PACKET_SIZE:
                self.readable.reset(raw, 0, current_len)
                return self.readable


    def set_server_thread_id(self, server_thread_id, host_address) -> None:
        is_master = host_address.primary if host_address is not None else None
        self.server_thread_log = "conn={} ({})".format(server_thread_id, is_master)
