from threading import RLock
from mariadb.client.Context import Context
from mariadb.client.PacketReader import PacketReader
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.message.server.ErrorPacket import ErrorPacket
from mariadb.util.constant import ServerStatus


class Result:
    __slots__ = ('closed', 'loaded', 'output_parameter', 'reader', 'exception_factory', 'context', 'cols', 'binary', 'meta_len', 'parser', 'res', 'parse_fcts')
    def __init__(self, binary_protocol: bool, metadata_list, reader: PacketReader, context: Context):
        self.reader = reader
        self.exception_factory = context.exception_factory
        self.context = context
        self.cols = metadata_list
        self.binary = binary_protocol
        self.closed = False
        self.loaded = False
        self.output_parameter = False

        self.meta_len = len(metadata_list)
        self.parser = self.decode_binary if binary_protocol else self.decode_text
        self.res = [None] * self.meta_len
        self.parse_fcts = [None] * self.meta_len
        for i, col in enumerate(self.cols):
            self.parse_fcts[i] = col.parser(binary_protocol)




    def read_next(self) -> tuple:
        buf = self.reader.read_packet()
        header = buf.get_unsigned_byte()
        if header == 0xFF:
            self.loaded = True
            self.res = None
            error_packet = ErrorPacket(buf, self.context)
            raise self.exception_factory.create(error_packet.message, error_packet.sql_state, error_packet.error_code)
        elif header == 0xFE and ((self.context.eof_deprecated and buf.readable_bytes() < 16777215) or (
                    not self.context.eof_deprecated and buf.readable_bytes() < 8)):
            if not self.context.eof_deprecated:
                # EOF_Packet
                # skip header + warning
                buf.skip(3)
                server_status = buf.read_unsigned_short()
            else:
                # OK_Packet with a 0xFE header
                # skip header
                buf.skip_one()
                buf.skip(buf.read_length_not_null())  # skip update count
                buf.skip(buf.read_length_not_null())  # skip insert id
                server_status = buf.read_unsigned_short()

            self.output_parameter = (server_status & ServerStatus.PS_OUT_PARAMETERS) != 0
            self.context.server_status = server_status
            self.loaded = True
            self.res = None
            return None
        else:
            return self.parser(buf)

    def decode_binary(self, buf: ReadableByteBuf) -> tuple:
        buf.skip_one()
        null_bitmap = buf.read_buffer(int((self.meta_len + 9) / 8))
        for i, parse_fct in enumerate(self.parse_fcts):
            if (null_bitmap[int((i + 2) / 8)] & (1 << ((i + 2) % 8))) > 0:
                self.res[i] = None
            else:
                self.res[i] = getattr(buf, parse_fct)()
        return tuple(self.res)

    def decode_text(self, buf: ReadableByteBuf) -> tuple:
        for i, parse_fct in enumerate(self.parse_fcts):
            self.res[i] = getattr(buf, parse_fct)()
        return tuple(self.res)

    def skip_remaining(self):
        while True:
            buf = self.reader.read_packet()
            header = buf.get_unsigned_byte()
            if header == 0xFF:
                self.loaded = True
                error_packet = ErrorPacket(buf, self.context)
                raise self.exception_factory.create(error_packet.message, error_packet.sql_state,
                                                    error_packet.error_code)
            elif header == 0xFE:
                if (self.context.eof_deprecated and len(buf) < 16777215) or (
                        not self.context.eof_deprecated and len(buf) < 8):
                    read_buf = ReadableByteBuf(None, buf, 0, buf.length)

                    if not self.context.eof_deprecated:
                        # EOF_Packet
                        read_buf.skip(3)  # skip header + warning
                        server_status = read_buf.read_unsigned_short()
                    else:
                        # OK_Packet with a 0xFE header
                        read_buf.skip_one()  # skip header
                        read_buf.skip(read_buf.read_length_not_null())  # skip update count
                        read_buf.skip(read_buf.read_length_not_null())  # skip insert id
                        server_status = read_buf.read_unsigned_short()

                    self.output_parameter = (server_status & ServerStatus.PS_OUT_PARAMETERS) != 0
                    self.context.server_status = server_status
                    self.loaded = True
                    return False

    def streaming(self) -> bool:
        pass

    def fetch_remaining(self) -> None:
        pass

    def close(self) -> None:
        if not self.loaded:
            try:
                self.skipRemaining()
            except Exception as err:
                raise self.exception_factory.create("Error while streaming resultSet data", "08000", err)

        self.closed = True

    def close_from_stmt_close(self, lock: RLock):
        lock.acquire()
        try:
            self.fetch_remaining()
            self.closed = True
        finally:
            lock.release()

    def abort(self) -> None:
        self.closed = True

    def close_from_stmt_close(self, lock: RLock):
        self.closed = True

    def fetchone(self) -> tuple:
        pass

    def fetchmany(self, arraysize: int = -1) -> tuple:
        pass

    def fetchall(self) -> tuple:
        pass

    def streaming(self) -> bool:
        pass
