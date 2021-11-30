from mariadb.client.Context import Context
from mariadb.client.PacketReader import PacketReader
from mariadb.client.PacketWriter import PacketWriter
from mariadb.client.ReadableByteBuf import ReadableByteBuf


class AuthenticationPlugin:

    def get_type(self) -> str:
        pass

    def initialize(self, user: str, password: str, seed: bytearray, conf) -> None:
        pass

    def process(self, writer: PacketWriter, reader: PacketReader, context: Context) -> ReadableByteBuf:
        pass
