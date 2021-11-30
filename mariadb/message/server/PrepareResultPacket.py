from mariadb.client.PacketReader import PacketReader
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.message.server.Column import Column
from mariadb.util.constant import Capabilities


class PrepareResultPacket:

    def __init__(self, buffer: ReadableByteBuf, reader: PacketReader, context, client):
        buffer.read_byte()
        self.client = client
        self.statement_id = buffer.read_int()
        num_columns = buffer.read_unsigned_short()
        num_params = buffer.read_unsigned_short()
        parameters = [None] * num_params
        self.columns = [None] * num_columns
        if num_params > 0:
            for i in range(num_params):
                parameters[i] = Column.decode(reader.read_packet(),
                                              context.server_capabilities & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO > 0)
            if not context.eof_deprecated:
                reader.read_packet()
        if num_columns > 0:
            for i in range(num_columns):
                self.columns[i] = Column.decode(reader.read_packet(),
                                                context.server_capabilities & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO > 0)

        if not context.eof_deprecated:
            reader.read_packet()

    def close(self, con) -> None:
        con.close_prepare(self)

    def decrement_use(self, con) -> None:
        self.close(con)
