from mariadb.client.Context import Context
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage


class QuitPacket(ClientMessage):

    def encode(self, writer: PacketWriter, context: Context) -> int:
        writer.init_packet()
        writer.write_byte(0x01)
        writer.flush()
        return 1
