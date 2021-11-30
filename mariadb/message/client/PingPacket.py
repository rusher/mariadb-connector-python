from mariadb.client.Context import Context
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage


class PingPacket(ClientMessage):

    def encode(self, writer: PacketWriter, context: Context) -> int:
        writer.init_packet()
        writer.write_byte(0x0e)
        writer.write_int(self.statement_id)
        writer.flush()
        return 1
