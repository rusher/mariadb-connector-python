from mariadb.client.Context import Context
from mariadb.client.DataType import DataType
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.ClientMessage import ClientMessage
from mariadb.message.client.ExecutePacket import param_datatype, write_param
from mariadb.util.ExceptionFactory import MaxAllowedPacketException


class BulkExecutePacket(ClientMessage):
    __slots__ = ('statement_id', 'parameters', 'sql')

    def __init__(self, statement_id: int, parameters, sql: str):
        self.batch_parameter_list = parameters
        self.statement_id = statement_id
        self.sql = sql

    def encode(self, writer: PacketWriter, context: Context) -> int:

        param_iterator = self.batch_parameter_list.copy()
        parameters = param_iterator.pop(0)
        parameter_count = len(parameters)

        parameter_header_type = [DataType] * parameter_count
        # set header type
        for i in range(parameter_count):
            parameter_header_type[i] = param_datatype(parameters[i])

        last_cmd_data = None
        bulk_packet_no = 0

        # Implementation After writing a bunch of parameter to buffer is marked. then : - when writing
        # next bunch of parameter, if buffer grow more than max_allowed_packet, send buffer up to mark,
        # then create a new packet with current bunch of data - if a bunch of parameter data type
        # changes
        # send buffer up to mark, then create a new packet with new data type.
        # Problem remains if a bunch of parameter is bigger than max_allowed_packet
        while parameters:
            bulk_packet_no += 1

            writer.init_packet()
            writer.write_byte(0xfa)  # COM_STMT_BULK_EXECUTE
            writer.write_int(self.statement_id)
            writer.write_short(128)  # always SEND_TYPES_TO_SERVER

            for i in range(parameter_count):
                writer.write_short(parameter_header_type[i].value)

            if last_cmd_data is not None:
                if writer.throw_max_allowed_length(last_cmd_data.length):
                    raise MaxAllowedPacketException("query size is >= to max_allowed_packet")

                writer.write_bytes(last_cmd_data, len(last_cmd_data))
                writer.mark()
                last_cmd_data = None
                if not param_iterator:
                    break

                parameters = param_iterator.pop(0)

            while True:
                for i in range(parameter_count):
                    param = parameters[i]
                    if param is None:
                        writer.write_byte(0x01)  # value is null
                    else:
                        writer.write_byte(0x00)  # value follow
                        write_param(writer, param)

                if not writer.is_marked() and writer.has_flushed():
                    # parameter were too big to fit in a MySQL packet
                    # need to finish the packet separately
                    writer.flush()
                    if not param_iterator:
                        parameters = None
                        break

                    parameters = param_iterator.pop(0)
                    # reset header type
                    for j in range(parameter_count):
                        parameter_header_type[j] = param_datatype(parameters[j])
                    break

                writer.mark_pos()

                if writer.buf_is_data_after_mark():
                    # flush has been done
                    last_cmd_data = writer.reset_mark()
                    break

                if not param_iterator:
                    parameters = None
                    break

                parameters = param_iterator.pop(0)

                # ensure type has not changed
                for i in range(parameter_count):
                    if parameter_header_type[i] != param_datatype(parameters[i]):
                        writer.flush()
                        # reset header type
                        for j in range(parameter_count):
                            parameter_header_type[j] = param_datatype(parameters[j])

                        break
                else:
                    continue

                break
        writer.flush()

        return bulk_packet_no

    def binary_protocol(self) -> bool:
        return True

    def description(self) -> str:
        return "BULK " + self.sql
