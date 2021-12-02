import logging
import socket
import struct
from threading import RLock

from mariadb.HostAddress import HostAddress
from mariadb.client.Context import Context
from mariadb.client.PacketReader import PacketReader
from mariadb.client.PacketWriter import PacketWriter
from mariadb.client.PrepareLruCache import PrepareLruCache
from mariadb.client.ReadAheadBufferedStream import ReadAheadBufferedStream
from mariadb.message.ClientMessage import ClientMessage
from mariadb.message.client.ClosePreparePacket import ClosePreparePacket
from mariadb.message.client.HandshakeResponse import HandshakeResponse
from mariadb.message.client.QuitPacket import QuitPacket
from mariadb.message.server.ErrorPacket import ErrorPacket
from mariadb.message.server.InitialHandshakePacket import InitialHandshakePacket
from mariadb.util.ExceptionFactory import ExceptionFactory, MaxAllowedPacketException, SQLError
from mariadb.util.constant import Capabilities, ServerStatus


class Client:
    logger = logging.getLogger(__name__)
    __slots__ = (
    'sequence', 'lock', 'conf', 'host_address', 'closed', 'stream_cursor', 'stream_msg',
    'reader', 'writer', 'socket', 'exception_factory', 'disable_pipeline', 'context')

    def __init__(self, conf, host_address: HostAddress, lock: RLock):
        self.sequence = bytearray(2)
        self.lock = lock
        self.conf = conf
        self.host_address = host_address
        self.closed = False
        self.stream_cursor = None
        self.stream_msg = None
        self.reader = None
        self.writer = None
        self.socket = None
        self.context = None
        self.exception_factory = ExceptionFactory(conf, host_address)
        self.disable_pipeline = conf.get("non_mapped_options").get("disablePipeline", False)
        host = host_address.host if host_address is not None else None

        # **********************************************************************
        # creating socket
        # **********************************************************************
        try:
            self.socket = self.connect_socket(conf, host_address)
            self.set_socket_option(conf)

            # **********************************************************************
            # assign reader/writer
            # **********************************************************************
            self.writer = PacketWriter(self.socket, conf.get('max_query_size_to_log'), self.sequence)
            self.writer.set_server_thread_id(-1, host_address)

            self.reader = PacketReader(ReadAheadBufferedStream(self.socket), conf, self.sequence)
            self.reader.set_server_thread_id(-1, host_address)

            # read server handshake
            buf = self.reader.get_packet_from_socket()
            if buf.get_byte() == -1:
                err = ErrorPacket(buf, None)
                raise self.exception_factory.create(err.message, err.sql_state, err.error_code)

            handshake = InitialHandshakePacket.decode(buf)
            self.exception_factory.thread_id = handshake.thread_id
            client_capabilities = self.initialize_client_capabilities(conf, handshake.capabilities)
            self.context = Context(handshake, client_capabilities, conf, self.exception_factory,
                                   PrepareLruCache(self.conf.get('prep_stmt_cache_size')))

            self.reader.set_server_thread_id(handshake.thread_id, host_address)
            self.writer.set_server_thread_id(handshake.thread_id, host_address)

            exchange_charset = self.decide_language(handshake)

            # **********************************************************************
            # changing to SSL socket if needed
            # **********************************************************************
            # TODO

            # **********************************************************************
            # handling authentication
            # **********************************************************************
            authentication_plugin_type = handshake.authentication_plugin_type
            HandshakeResponse(conf.get('user'), conf.get('password'), authentication_plugin_type, self.context.seed,
                              conf,
                              host,
                              client_capabilities, exchange_charset).encode(self.writer, self.context)
            self.writer.flush()

            # **********************************************************************
            # activate compression if required
            # **********************************************************************
            # TODO

            buf = self.reader.get_packet_from_socket()
            header = buf.get_byte() & 0xFF
            if header == 0xFE:
                # *************************************************************************************
                # Authentication Switch Request see
                # https://mariadb.com/kb/en/library/connection/#authentication-switch-request
                # *************************************************************************************
                raise Exception('TODO')
            elif header == 0xFF:
                # *************************************************************************************
                # ERR_Packet
                # see https://mariadb.com/kb/en/library/err_packet/
                # *************************************************************************************
                error_packet = ErrorPacket(buf, self.context)
                raise self.context.exception_factory.create(error_packet.message, error_packet.sql_state,
                                                            error_packet.error_code)
            elif header == 0x00:
                # *************************************************************************************
                # OK_Packet -> Authenticated !
                # see https://mariadb.com/kb/en/library/ok_packet/
                # *************************************************************************************
                buf.skip_one()
                buf.skip(buf.read_length_not_null())
                buf.skip(buf.read_length_not_null())
                # insertId
                self.context.server_status = buf.read_short()
            else:
                raise self.context.exception_factory.create(
                    "unexpected data during authentication (header=" + header + ")",
                    "08000")

            # **********************************************************************
            # post queries
            # **********************************************************************
            # TODO if needed

        except Exception as err:
            self.destroy_socket()
            raise err

    def connect_socket(self, conf: dict, host_address: HostAddress):
        if conf.get("pipe") is None and conf.get('local_socket') is None and host_address is None:
            raise ExceptionFactory.SQLError("hostname must be set to connect socket if not using local socket or pipe")
        # if conf.get("pipe") is not None:
        #     return NamedPipeSocket(hostAddress != null ? hostAddress.host : null, conf.pipe());
        if conf.get('local_socket') is not None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(conf.get('local_socket'))
            return sock
        if host_address is None:
            raise ExceptionFactory.SQLError("hostname must be set to connect socket")
        kwargs = {}
        if conf.get('local_socket_address') is not None:
            kwargs["source_address"] = (conf.get('local_socket_address'), 0)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host_address.host, host_address.port))
        return s

    def set_socket_option(self, conf):
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket.settimeout(self.conf.get("socket_timeout", 30))
        if conf.get('tcp_keep_alive'):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if conf.get('tcp_abortive_close'):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))

    def initialize_client_capabilities(self, conf, server_capabilities):
        capabilities = \
            Capabilities.IGNORE_SPACE \
            | Capabilities.CLIENT_PROTOCOL_41 \
            | Capabilities.TRANSACTIONS \
            | Capabilities.SECURE_CONNECTION \
            | Capabilities.MULTI_RESULTS \
            | Capabilities.PS_MULTI_RESULTS \
            | Capabilities.PLUGIN_AUTH \
            | Capabilities.CONNECT_ATTRS \
            | Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA \
            | Capabilities.CLIENT_SESSION_TRACK \
            | Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO

        # since skipping metadata is only available when using binary protocol,
        # only set it when server permit it and using binary protocol
        if conf.get('use_binary') and conf.get("non_mapped_options").get("enableSkipMeta", True) and (
                server_capabilities & Capabilities.MARIADB_CLIENT_CACHE_METADATA) != 0:
            capabilities |= Capabilities.MARIADB_CLIENT_CACHE_METADATA

        if conf.get('use_bulk'):
            capabilities |= Capabilities.MARIADB_CLIENT_STMT_BULK_OPERATIONS

        if not conf.get('use_affected_rows'):
            capabilities |= Capabilities.FOUND_ROWS

        if conf.get('allow_multi_queries'):
            capabilities |= Capabilities.MULTI_STATEMENTS

        if conf.get('allow_local_infile'):
            capabilities |= Capabilities.LOCAL_FILES

        # useEof is a technical option
        deprecate_eof = conf.get("non_mapped_options").get("deprecateEof", True)
        if (server_capabilities & Capabilities.CLIENT_DEPRECATE_EOF) != 0 and deprecate_eof:
            capabilities |= Capabilities.CLIENT_DEPRECATE_EOF

        if conf.get('use_compression') and (server_capabilities & Capabilities.COMPRESS) != 0:
            capabilities |= Capabilities.COMPRESS

        if conf.get('database') is not None:
            capabilities |= Capabilities.CONNECT_WITH_DB

        return capabilities

    def decide_language(self, handshake: InitialHandshakePacket) -> int:
        server_language = handshake.default_collation
        # return current server utf8mb4 collation
        if server_language == 45 or server_language == 46 or (224 <= server_language <= 247):
            return server_language
        return 224

    def execute_pipeline(self, messages: list, stmt=None, fetch_size: int = 0) -> list:
        self.check_not_closed()
        results = []
        read_counter = 0
        response_msg = [0] * len(messages)
        try:
            if self.disable_pipeline:
                for msg in messages:
                    results.extend(self.execute(msg, stmt, fetch_size))
            else:
                for i, msg in enumerate(messages):
                    if Client.logger.isEnabledFor(logging.DEBUG):
                        Client.logger.debug("execute query: {}".format(msg.description()))
                    response_msg[i] = msg.encode(self.writer, self.context)
                while read_counter < len(messages):
                    read_counter += 1
                    for j in range(response_msg[read_counter - 1]):
                        results.extend(self.read_response(messages[read_counter - 1], stmt, fetch_size))
            return results

        except MaxAllowedPacketException as e:
            raise self.exception_factory.create(
                "Packet too big for current server max_allowed_packet value", "HZ000", e)
            destroy_socket()
        except Exception as e:
            if not self.closed:
                # read remaining results
                for i in range(read_counter, len(messages)):
                    for j in range(response_msg[i]):
                        try:
                            results.extend(self.read_response(messages[i], stmt, fetch_size))
                        except Exception as e2:
                            pass
            raise

    def execute(self, message: ClientMessage, cursor=None, fetch_size: int = 0) -> list:
        """
        Execute one command, and read response
        :param message: command to execute
        :param cursor: current cursor
        :param fetch_size: result-set size to read if streaming
        :return: command response
        """
        self.check_not_closed()

        if Client.logger.isEnabledFor(logging.DEBUG):
            Client.logger.debug("execute query: {}".format(message.description()))

        try:
            nb_resp = message.encode(self.writer, self.context)
            if nb_resp == 1:
                return self.read_response(message, cursor, fetch_size)
            else:
                # Bulk Command that was too big, separate into multiple ones
                if self.stream_cursor is not None:
                    self.stream_cursor.fetchRemaining()
                    self.stream_cursor = None
                server_msgs = []
                while nb_resp > 0:
                    nb_resp -= 1
                    server_msgs.append(self.read_msg_result(cursor, message, fetch_size))
                    while (self.context.server_status & ServerStatus.MORE_RESULTS_EXISTS) > 0:
                        server_msgs.append(self.read_msg_result(cursor, message, fetch_size))

            return server_msgs
        except MaxAllowedPacketException as e:
            raise self.exception_factory.with_sql(message.description()).create(
                "Packet too big for current server max_allowed_packet value", "HZ000", e)
            destroy_socket()
        except SQLError as sqle:
            raise self.exception_factory.with_sql(message.description()).create("Socket error", "08000", sqle)


    def read_response(self, message: ClientMessage, stmt=None, fetch_size: int = 0) -> list:
        self.check_not_closed()
        if self.stream_cursor is not None:
            self.stream_cursor.fetchRemaining()
            self.stream_cursor = None
        server_msgs = []
        server_msgs.append(self.read_msg_result(stmt, message, fetch_size))
        while (self.context.server_status & ServerStatus.MORE_RESULTS_EXISTS) > 0:
            server_msgs.append(self.read_msg_result(stmt, message, fetch_size))
        return server_msgs


    def close_prepare(self, prepare) -> None:
        self.check_not_closed()
        try:
            ClosePreparePacket(prepare.statement_id).encode(self.writer, self.context)
        except Exception as e:
            self.destroy_socket()
            raise self.exception_factory.create("Socket error during post connection queries: " + e.getMessage(),
                                                "08000", e)

    def read_streaming_results(self, cursor_result):
        """
        If last command was a streaming result-set not completely read, fetch remaining packets into streaming cursor
        results before reading current response
        :param cursor_result: cursor result
        :return:
        """
        if self.stream_cursor is not None:
            cursor_result.append(self.read_msg_result(self.stream_cursor, self.stream_msg, 0))
            while (self.context.server_status & ServerStatus.MORE_RESULTS_EXISTS) > 0:
                cursor_result.append(self.read_msg_result(self.stream_cursor, self.stream_msg, 0))

    def read_msg_result(self, cursor, message: ClientMessage, fetch_size: int):
        """
        read message result: i.e. an OkPacket, a Result for a result-set ...
        :param cursor: current cursor
        :param message: message to send
        :param fetch_size: streaming value
        :return: message result-set
        """
        server_msg = message.read_msg_result(
            cursor,
            fetch_size,
            self.reader,
            self.writer,
            self.context,
            self.exception_factory)
        if not server_msg.loaded():
            self.stream_cursor = cursor
            self.stream_msg = message
        return server_msg

    def destroy_socket(self) -> None:
        self.closed = True
        if self.writer is not None:
            self.writer.close()
        try:
            if self.socket is not None:
                self.socket.shutdown(socket.SHUT_WR)
                self.socket.close()
        except Exception:
            pass

    def check_not_closed(self) -> None:
        if self.closed:
            raise self.exception_factory.create("Connection is closed", "08000", 1220)

    def close(self):
        self.lock.acquire()
        try:
            if not self.closed:
                self.closed = True
            try:
                quit_packet = QuitPacket()
                quit_packet.encode(self.writer, self.context)
            except Exception:
                pass
            self.close_socket()

        finally:
            self.lock.release()

    def close_socket(self) -> None:
        try:
            self.socket.settimeout(3)
            self.socket.close()
            self.writer.close()
        except Exception as e:
            pass
