import struct

from mariadb.client.PacketReader import PacketReader
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.message.server.Column import Column
from mariadb.util.constant import Capabilities

PARSER = struct.Struct("<IHH")

class PrepareResultPacket:

    __slots__ = ('client', 'statement_id', 'num_params', 'columns')

    def __init__(self, buffer: ReadableByteBuf, reader: PacketReader, context, client):
        buffer.read_byte()
        self.client = client
        self.statement_id, num_columns, self.num_params = PARSER.unpack_from(buffer.buf, buffer.pos)
        parameters = [None] * self.num_params
        self.columns = [None] * num_columns
        if self.num_params > 0:
            for i in range(self.num_params):
                parameters[i] = Column.decode(reader.get_packet_from_socket(),
                                              context.server_capabilities & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO > 0)
            if not context.eof_deprecated:
                reader.get_packet_from_socket()
        if num_columns > 0:
            for i in range(num_columns):
                self.columns[i] = Column.decode(reader.get_packet_from_socket(),
                                                context.server_capabilities & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO > 0)

        if not context.eof_deprecated:
            reader.get_packet_from_socket()

    def close(self, con) -> None:
        con.close_prepare(self)

    def decrement_use(self, con) -> None:
        self.close(con)

    def streaming(self) -> bool:
        return False
