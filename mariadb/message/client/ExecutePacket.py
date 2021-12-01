from datetime import datetime, date, time

from mariadb.client.Context import Context
from mariadb.client.DataType import DataType
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage
from mariadb.message.client.LongDataPacket import LongDataPacket

NO_CURSOR_AND_ITERATION = b'\x00\x01\x00\x00\x00'

class ExecutePacket(ClientMessage):
    __slots__ = ('statement_id', 'parameters', 'sql')

    def __init__(self, statement_id: int, parameters, sql: str):
        self.parameters = parameters
        self.statement_id = statement_id
        self.sql = sql

    def encode(self, writer: PacketWriter, context: Context) -> int:

        parameter_count = len(self.parameters)

        # send long data value in separate packet
        for i, param in enumerate(self.parameters):
            if param is not None and (type(param) is bytes or type(param) is bytearray or type(param) is memoryview):
                LongDataPacket(self.statement_id, param, i).encode(writer, context)

        writer.init_packet()
        writer.write_byte(0x17)
        writer.write_int(self.statement_id)
        writer.write_bytes(NO_CURSOR_AND_ITERATION, 5)  # NO CURSOR and 1 as Iteration pos

        if parameter_count > 0:
            # create null bitmap and reserve place in writer
            null_count = int((parameter_count + 7) / 8)
            null_bits_buffer = bytes(null_count)
            initial_pos = writer.pos
            writer.pos = initial_pos + null_count

            # Send Parameter type flag
            writer.write_byte(0x01)

            # Store types of parameters in first package that is sent to the server.
            for i, p in enumerate(self.parameters):
                if p is None:
                    null_bits_buffer[int(i / 8)] |= (1 << (i % 8))
                else:
                    writer.write_byte(param_datatype(p).value)
                    writer.write_byte(0)

            # write nullBitsBuffer in reserved place
            writer.write_bytes_at_pos(null_bits_buffer, initial_pos)

            # send not null parameter, not long data
            for i, p in enumerate(self.parameters):
                if p is None or type(p) is bytes or type(p) is bytearray or type(p) is memoryview:
                    continue
                write_param(writer, p)
        writer.flush()
        return 1

    def binary_protocol(self) -> bool:
        return True

    def can_skip_meta(self) -> bool:
        return True

    def description(self) -> str:
        return "EXECUTE " + self.sql


def write_param(writer: PacketWriter, param) -> None:
    if type(param) is bool:
        writer.write_byte(0x01 if param else 0x00)
    elif type(param) is str:
        b = param.encode('utf-8')
        writer.write_length(len(b))
        writer.write_bytes(b, len(b))
    elif type(param) is int:
        if -2147483648 < param < +2147483647:
            writer.write_int(param)
        else:
            writer.write_long(param)
    elif type(param) is float:
        b = str(param).encode('ascii')
        writer.write_length(len(b))
        writer.write_bytes(b, len(b))

    elif type(param) is datetime:
        if param.microsecond == 0:
            writer.write_byte(7)
            writer.write_short(param.year)
            writer.write_byte(param.month)
            writer.write_byte(param.day)
            writer.write_byte(param.hour)
            writer.write_byte(param.minute)
            writer.write_byte(param.second)
        else:
            writer.write_byte(11)
            writer.write_short(param.year)
            writer.write_byte(param.month)
            writer.write_byte(param.day)
            writer.write_byte(param.hour)
            writer.write_byte(param.minute)
            writer.write_byte(param.second)
            writer.write_int(param.microsecond)

    elif type(param) is date:
        writer.write_byte(4)
        writer.write_short(param.year)
        writer.write_byte(param.month)
        writer.write_byte(param.day)
    elif type(param) is time:
        if param.microsecond == 0:
            writer.write_byte(8)
            writer.write_byte(0)
            writer.write_int(0)
            writer.write_byte(param.hour)
            writer.write_byte(param.minute)
            writer.write_byte(param.second)
        else:
            writer.write_byte(12)
            writer.write_byte(0)
            writer.write_int(0)
            writer.write_byte(param.hour)
            writer.write_byte(param.minute)
            writer.write_byte(param.second)
            writer.write_int(param.microsecond)
    elif type(param) is bytes or type(param) is bytearray or type(param) is memoryview:
        writer.write_length(len(param))
        writer.write_bytes(param, len(param))
    else:
        raise Exception('type ' + type(param) + ' is not supported')


def param_datatype(p) -> DataType:
    if type(p) is bool:
        return DataType.TINYINT
    elif type(p) is str or type(p) is dict:
        return DataType.VARSTRING
    elif type(p) is int:
        if -2147483648 < p < +2147483647:
            return DataType.INTEGER
        else:
            return DataType.BIGINT
    elif type(p) is float:
        return DataType.DECIMAL
    elif type(p) is datetime:
        return DataType.DATETIME
    elif type(p) is date:
        return DataType.DATE
    elif type(p) is time:
        return DataType.TIME
    elif type(p) is bytes or type(p) is bytearray or type(p) is memoryview:
        return DataType.BLOB
    raise Exception('type ' + type(p) + ' is not supported')
