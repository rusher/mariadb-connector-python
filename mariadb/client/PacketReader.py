import copy
import logging

from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.client.util.MutableInt import MutableInt
from mariadb.util import LoggerHelper

MAX_PACKET_SIZE = 0xffffff


class PacketReader:
    logger = logging.getLogger(__name__)

    __slots__ = ('stream', 'max_query_size_to_log', 'sequence', 'server_thread_log', 'readable')

    def __init__(self, stream, conf, sequence: MutableInt):
        self.stream = stream
        self.max_query_size_to_log = conf.get('max_query_size_to_log')
        self.sequence = sequence
        self.server_thread_log = ""
        self.readable = ReadableByteBuf(self.sequence, bytearray(), 0, 0)

    def read_packet(self) -> ReadableByteBuf:
        # ***************************************************
        # Read 4 byte header
        # ***************************************************
        last_packet_length = self.read_header()

        # ***************************************************
        # Read content
        # ***************************************************
        raw, pos, end = self.stream.read(last_packet_length)

        if PacketReader.logger.isEnabledFor(logging.DEBUG):
            b = bytearray(4)
            b[0] = last_packet_length & 0xFF
            b[1] = last_packet_length >> 8
            b[2] = last_packet_length >> 16
            b[3] = self.sequence.value & 0xFF
            trace = LoggerHelper.hex_header(b, raw, pos, end - pos, self.max_query_size_to_log)
            PacketReader.logger.debug("read: " + self.server_thread_log + "\n" + trace)

        if last_packet_length < MAX_PACKET_SIZE:
            self.readable.reset(raw, pos, end)
            return self.readable

        # ***************************************************
        # In case content length is big, content will be separate in many 16Mb packets
        # ***************************************************
        while True:
            last_packet_length = self.read_header()

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
                b[0] = last_packet_length & 0xFF
                b[1] = last_packet_length >> 8
                b[2] = last_packet_length >> 16
                b[3] = self.sequence.value & 0xFF
                trace = LoggerHelper.hex_header(b, raw, 0, last_packet_length, self.max_query_size_to_log)
                PacketReader.logger.debug("read: " + self.server_thread_log + "\n" + trace)
            if last_packet_length < MAX_PACKET_SIZE:
                self.readable.reset(raw, 0, current_len)
                return self.readable


    def set_server_thread_id(self, server_thread_id, host_address) -> None:
        is_master = host_address.primary if host_address is not None else None
        self.server_thread_log = "conn={} ({})".format(server_thread_id, is_master)

    def read_header(self):
        header, pos_header, end_header = self.stream.read(4)
        last_packet_length = (header[pos_header] & 0xff) + ((header[pos_header + 1] & 0xff) << 8) + ((header[pos_header + 2] & 0xff) << 16)
        self.sequence.value = header[pos_header + 3]
        return last_packet_length