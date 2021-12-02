from mariadb.client.Context import Context
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage


class QueryPacket(ClientMessage):

    __slots__ = ('sql')

    def __init__(self, sql: str):
        self.sql = sql

    def encode(self, writer: PacketWriter, context: Context) -> int:
        sql_bytes = self.sql.encode()
        writer.init_packet()
        writer.write_byte(0x03)
        writer.write_bytes(sql_bytes, len(sql_bytes))
        writer.flush()
        return 1

    def description(self) -> str:
        return self.sql
