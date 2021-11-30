from mariadb.client.PacketReader import PacketReader
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.message.server.PrepareResultPacket import PrepareResultPacket


class CachedPrepareResultPacket(PrepareResultPacket):

    def __init__(self, buffer: ReadableByteBuf, reader: PacketReader, context, client):
        super(CachedPrepareResultPacket, self).__init__(buffer, reader, context, client)
        self.closing = False
        self.cached = False

    def close(self, con) -> None:
        if not self.cached and not self.closing:
            self.closing = True
            con.close_prepare(self)

    def decrement_use(self, con) -> None:
        self.close(con)
        if not self.cached:
            self.close(con)

    def increment_use(self) -> None:
        if self.closing:
            return

    def un_cache(self) -> None:
        self.cached = False
        try:
            self.close(self.client)
        finally:
            # eat
            return

    def cache(self) -> bool:
        if self.closing:
            return False
        newly_cached = self.cached is False
        self.cached = True
        return newly_cached

    def reset(self) -> None:
        pass
