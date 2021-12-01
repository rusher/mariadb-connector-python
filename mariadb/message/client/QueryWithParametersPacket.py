import json
from datetime import datetime, date, time

from mariadb.client.Context import Context
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage
from mariadb.util.ClientParser import ClientParser
from mariadb.util.constant import ServerStatus


class QueryWithParametersPacket(ClientMessage):

    __slots__ = ('parser', 'parameters')

    def __init__(self, parser: ClientParser, parameters):
        self.parser = parser
        self.parameters = parameters

    def encode(self, writer: PacketWriter, context: Context) -> int:

        no_backslash_escapes = (context.server_status & ServerStatus.NO_BACKSLASH_ESCAPES) > 0
        writer.init_packet()
        writer.write_byte(0x03)
        writer.write_bytes(self.parser.query_parts[0], len(self.parser.query_parts[0]))
        if self.parser.param_count > 0:
            for i in range(self.parser.param_count):
                param = self.parameters[i]
                if param is None:
                    writer.write_bytes(b"null", 4)
                else:
                    write_param(writer, param, no_backslash_escapes)
                writer.write_bytes(self.parser.query_parts[i + 1], len(self.parser.query_parts[i + 1]))
        writer.flush()
        return 1

    def description(self) -> str:
        return self.parser.sql


QUOTE = ord('\'')
COMA = ord(',')
BINARY_PREFIX = b'_BINARY \''


def write_param(writer: PacketWriter, param, no_backslash_escapes) -> None:
    if type(param) is bool:
        writer.write_bytes(b'true', 4) if param else writer.write_bytes(b'false', 5)
    elif type(param) is str:
        writer.write_byte(QUOTE)
        writer.write_string_escaped(param, no_backslash_escapes)
        writer.write_byte(QUOTE)
    elif type(param) is int or type(param) is float:
        writer.write_ascii(str(param))
    elif type(param) is datetime:
        writer.write_byte(QUOTE)
        writer.write_ascii(param.isoformat(sep=' '))
        writer.write_byte(QUOTE)
    elif type(param) is date:
        writer.write_byte(QUOTE)
        writer.write_ascii(param.isoformat())
        writer.write_byte(QUOTE)
    elif type(param) is time:
        # ensure not having +HH in isostring
        tmp_time = time(hour=param.hour, minute=param.minute, second=param.second, microsecond=param.microsecond)
        writer.write_byte(QUOTE)
        writer.write_ascii(param.isoformat())
        writer.write_byte(QUOTE)
    elif type(param) is list or type(param) is set or type(param) is frozenset:
        for idx, p in param:
            if idx != 0:
                writer.write_byte(COMA)
            write_param(writer, p)
    elif type(param) is bytes or type(param) is bytearray or type(param) is memoryview:
        writer.write_bytes(BINARY_PREFIX, len(BINARY_PREFIX))
        writer.write_bytes(param, len(param))
        writer.write_byte(QUOTE)
    elif type(param) is dict:
        writer.write_byte(QUOTE)
        writer.write_string_escaped(json.dumps(param), no_backslash_escapes)
        writer.write_byte(QUOTE)
    else:
        raise Exception('type ' + type(param) + ' is not supported')
