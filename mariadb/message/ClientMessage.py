from threading import RLock

from mariadb.client.Context import Context
from mariadb.client.PacketReader import PacketReader
from mariadb.client.PacketWriter import PacketWriter
from mariadb.client.result.CompleteResult import CompleteResult
from mariadb.message.server.Column import Column
from mariadb.message.server.ErrorPacket import ErrorPacket
from mariadb.message.server.OkPacket import OkPacket
from mariadb.util.ExceptionFactory import ExceptionFactory


class ClientMessage:

    def encode(self, writer: PacketWriter, context: Context) -> int:
        pass

    def batch_update_length(self) -> int:
        return 0

    def description(self) -> str:
        return None

    def binary_protocol(self) -> bool:
        return False

    def can_skip_meta(self) -> bool:
        return False

    def read_completion(self, cursor, fetch_size: int, reader: PacketReader, writer: PacketWriter,
                    context: Context, exception_factory: ExceptionFactory, lock: RLock):
        buf = reader.read_packet()
        header = buf.get_unsigned_byte()
        if header == 0x00:
            # *********************************************************************************************************
            # OK response
            # *********************************************************************************************************
            return OkPacket(buf, context)
        elif header == 0xff:
            # *********************************************************************************************************
            # ERROR response
            # *********************************************************************************************************

            # force current status to in transaction to ensure rollback/commit, since command may
            # have issue a transaction
            error_packet = ErrorPacket(buf, context)
            raise exception_factory.with_sql(self.description()).create(error_packet.message, error_packet.sql_state,
                                                                        error_packet.error_code)
        elif header == 0xfb:
            buf.skip(1)
            file_name = buf.read_string_null_end()
            try:
                with open(file_name, 'rb') as f:
                    contents = f.read()
                    writer.write_bytes(contents, 0, len(contents))
                writer.flush()

                writer.write_empty_packet()
                return self.read_packet(
                    cursor,
                    fetch_size,
                    reader,
                    writer,
                    context,
                    exception_factory,
                    lock)
            except IOError as f:
                writer.write_empty_packet()
                self.read_packet(
                    cursor,
                    fetch_size,
                    reader,
                    writer,
                    context,
                    exception_factory,
                    lock)
                raise exception_factory.with_sql(self.description()).create("Could not send file : " + f.getMessage(),
                                                                            "HY000", f)

        else:
            # ********************************************************************************************************
            # ResultSet
            # ********************************************************************************************************
            field_count = buf.read_length_not_null()

            can_skip_meta = context.skip_meta and self.can_skip_meta()
            skip_meta = False if not can_skip_meta else buf.read_byte() == 0
            if can_skip_meta and skip_meta:
                ci = cursor.prepare.columns
            else:
                # read columns information's
                ci = [None] * field_count
                for i in range(field_count):
                    ci[i] = Column.decode(reader.read_packet(), context.extended_info)

            if can_skip_meta and not skip_meta:
                cursor.update_meta(ci)

            # intermediate EOF
            if not context.eof_deprecated:
                reader.read_packet()

            # read resultSet
            # if fetch_size != 0:
            #     if (context.server_status & ServerStatus.MORE_RESULTS_EXISTS) > 0:
            #         context.server_status = context.server_status - ServerStatus.MORE_RESULTS_EXISTS
            #
            #     return StreamingResult(
            #         self.binary_protocol(),
            #         ci,
            #         reader,
            #         context,
            #         fetch_size,
            #         lock)
            # else:
            return CompleteResult(
                self.binary_protocol(),
                ci,
                reader,
                context)
