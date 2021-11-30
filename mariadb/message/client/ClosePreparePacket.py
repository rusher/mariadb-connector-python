from mariadb.client.Context import Context
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage


class ClosePreparePacket(ClientMessage):

    def __init__(self, statement_id: int):
        self.statement_id = statement_id

    def encode(self, writer: PacketWriter, context: Context) -> int:
        writer.init_packet()
        writer.write_byte(0x19)
        writer.write_int(self.statement_id)
        writer.flush()
        return 0
