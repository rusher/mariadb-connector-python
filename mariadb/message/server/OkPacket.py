import logging

from mariadb.client.Context import Context
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.util.constant import Capabilities, StateChange


class OkPacket:
    logger = logging.getLogger(__name__)

    __slots__ = ('affected_rows', 'last_insert_id')

    def __init__(self, buf: ReadableByteBuf, context: Context):
        buf.skip_one()
        self.affected_rows = buf.read_length_not_null()
        self.last_insert_id = buf.read_length_not_null()
        context.server_status = buf.read_unsigned_short()
        # warning
        buf.skip(2)

        if (context.server_capabilities & Capabilities.CLIENT_SESSION_TRACK) != 0 and buf.readable_bytes() > 0:
            buf.skip(buf.read_length_not_null())  # skip info
            while buf.readable_bytes() > 0:
                if buf.read_length_not_null() > 0:
                    header = buf.read_byte()
                    if header == StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
                        buf.read_length_not_null()
                        variable = buf.read_string(buf.read_length_not_null())
                        length = buf.read_length()
                        value = None if length is None else length
                        if OkPacket.logger.isEnabledFor(logging.DEBUG):
                            OkPacket.logger.debug("System variable change:  {} = {}".format(variable, value))
                        break
                    elif header == StateChange.SESSION_TRACK_SCHEMA:
                        buf.read_length_not_null()
                        db_len = buf.read_length()
                        database = None if db_len == None else buf.read_string(db_len)
                        context.database = None if database == "" else database
                        if OkPacket.logger.isEnabledFor(logging.DEBUG):
                            OkPacket.logger.debug("Database change: is '{}'".format(database))
                        break
                    else:
                        buf.skip(buf.read_length_not_null())
