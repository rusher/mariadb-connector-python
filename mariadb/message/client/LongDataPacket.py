from mariadb.client.Context import Context
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage


class LongDataPacket(ClientMessage):
    def __init__(self, statement_id: int, parameter, index: int):
        self.statement_id = statement_id
        self.parameter = parameter
        self.index = index

    def encode(self, writer: PacketWriter, context: Context) -> int:
        writer.init_packet()
        writer.write_byte(0x18)
        writer.write_int(self.statement_id)
        writer.write_short(self.index)
        writer.write_bytes(self.parameter, len(self.parameter))
        writer.flush()
        return 0
