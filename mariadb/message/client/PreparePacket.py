from threading import RLock

from mariadb.client.Context import Context
from mariadb.client.PacketReader import PacketReader
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage
from mariadb.message.server.CachedPrepareResultPacket import CachedPrepareResultPacket
from mariadb.message.server.ErrorPacket import ErrorPacket
from mariadb.message.server.PrepareResultPacket import PrepareResultPacket
from mariadb.util.ExceptionFactory import ExceptionFactory


class PreparePacket(ClientMessage):

    __slots__ = ('sql', 'client')

    def __init__(self, sql: str, client):
        self.sql = sql
        self.client = client

    def encode(self, writer: PacketWriter, context: Context) -> int:
        sql_bytes = self.sql.encode()
        writer.init_packet()
        writer.write_byte(0x16)
        writer.write_bytes(sql_bytes, len(sql_bytes))
        writer.flush()
        return 1

    def read_msg_result(self, cursor, fetch_size: int, reader: PacketReader, writer: PacketWriter,
                        context: Context, exception_factory: ExceptionFactory):

        buf = reader.get_packet_from_socket()
        #*********************************************************************************************************
        # ERROR response
        #*********************************************************************************************************
        if buf.get_unsigned_byte() == 0xff:
            # force current status to in transaction to ensure rollback/commit, since
            # command may
            # have issue a transaction
            error_packet = ErrorPacket(buf, context)
            raise exception_factory.with_sql(self.description()).create(error_packet.message, error_packet.sql_state, error_packet.error_code)

        if context.conf.get('use_binary') and context.conf.get('prep_stmt_cache_size') > 0 and len(self.sql) < 8192:
            prepare = CachedPrepareResultPacket(buf, reader, context, self.client)
            previous_cached = context.prepare_cache.put(self.sql, prepare)
            cursor.prepare = previous_cached if previous_cached is not None else prepare
        else:
            cursor.prepare = PrepareResultPacket(buf, reader, context, self.client)
        return cursor.prepare


    def description(self) -> str:
        return "PREPARE " + self.sql
