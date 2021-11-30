import logging

from mariadb.client.Context import Context
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.util.constant import ServerStatus


class ErrorPacket:
    logger = logging.getLogger(__name__)

    def __init__(self, buf: ReadableByteBuf, context: Context):
        buf.skip()
        self.error_code = buf.read_short()
        next = buf.get_byte(buf.pos)
        if next == '#':
            buf.skip()
            self.sql_state = buf.read_ascii(5)
            self.message = buf.read_string_eof()
        else:
            # Pre-4.1 message, still can be output in newer versions (e.g. with 'Too many connections')
            self.message = buf.read_string_eof()
            self.sql_state = "HY000"

        if ErrorPacket.logger.isEnabledFor(logging.INFO):
            ErrorPacket.logger.debug("Error: %i-%s: %s", self.error_code, self.sql_state, self.message)

        # force current status to in transaction to ensure rollback/commit, since command may have
        # issue a transaction
        if context is not None:
            server_status = context.server_status
            server_status |= ServerStatus.IN_TRANSACTION
            context.server_status = server_status
