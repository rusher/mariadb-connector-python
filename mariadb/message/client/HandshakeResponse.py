import platform
from getpass import getpass

from mariadb.client.Context import Context
from mariadb.client.PacketWriter import PacketWriter
from mariadb.message.authentication.NativePasswordPlugin import NativePasswordPlugin
from mariadb.util.constant import Capabilities

_CLIENT_NAME = "_client_name"
_CLIENT_VERSION = "_client_version"
_SERVER_HOST = "_server_host"
_OS = "_os"
_PYTHON_VERSION = "_python_version"


def write_string_length(encoder: PacketWriter, value: str):
    val_bytes = value.encode('utf-8')
    encoder.write_length(len(val_bytes))
    encoder.write_bytes(val_bytes, len(val_bytes))


def write_string_length_ascii(encoder: PacketWriter, value: str):
    ascii_val = value.encode('ascii')
    encoder.write_bytes(ascii_val, len(ascii_val))


def write_connect_attributes(writer: PacketWriter, connection_attributes: str, host: str):
    writer.mark_pos()
    writer.write_int(0)

    write_string_length_ascii(writer, _CLIENT_NAME)
    write_string_length(writer, "MariaDB Connector/J")

    write_string_length_ascii(writer, _CLIENT_VERSION)
    write_string_length(writer, "0.0.1")

    write_string_length_ascii(writer, _SERVER_HOST)
    write_string_length(writer, host if host is not None else "")

    write_string_length_ascii(writer, _OS)
    write_string_length(writer, platform.platform())

    write_string_length_ascii(writer, _PYTHON_VERSION)
    write_string_length(writer, platform.python_version())

    if connection_attributes is not None:
        tokens = connection_attributes.strip().split(",")
        for token in tokens:
            separator = token.find(":")
            if separator != -1:
                write_string_length(writer, token.substring(0, separator))
                write_string_length(writer, token.substring(separator + 1))
            else:
                write_string_length(writer, token)
                write_string_length(writer, "")
    # write real length
    ending = writer.pos
    writer.reset_mark()
    length = ending - (writer.pos + 4)

    writer.write_byte(0xfd)
    writer.write_bytes(length.to_bytes(3, byteorder='little', signed=False), 3)
    writer.pos = ending


class HandshakeResponse:

    def __init__(self, user: str, password: str, authentication_plugin_type: str, seed: bytearray, conf, host: str,
                 client_capabilities: int, exchange_charset: int):
        self.authentication_plugin_type = authentication_plugin_type
        self.seed = seed
        self.user = user
        self.password = password
        self.database = conf.get('database')
        self.connection_attributes = conf.get('connection_attributes')
        self.host = host
        self.client_capabilities = client_capabilities
        self.exchange_charset = exchange_charset

    def encode(self, writer: PacketWriter, context: Context) -> None:

        if "mysql_clear_password" == self.authentication_plugin_type:
            if (self.client_capabilities & Capabilities.SSL) == 0:
                raise Exception("Cannot send password in clear if SSL is not enabled.")

            auth_data = [] if self.password is None else self.password.encode('utf-8')
        else:
            self.authentication_plugin_type = "mysql_native_password"
            auth_data = NativePasswordPlugin.encrypt_password(self.password, self.seed)

        writer.write_int(self.client_capabilities % (1 << 32))
        writer.write_int(1024 * 1024 * 1024)
        writer.write_byte(self.exchange_charset)
        writer.write_bytes(bytearray([0x00] * 19), 19)
        # Maria extended flag
        writer.write_int(self.client_capabilities >> 32)
        writer.write_string(self.user if self.user is not None else getpass.getuser())
        writer.write_byte(0x00)

        if (context.server_capabilities & Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0:
            writer.write_length(len(auth_data))
            writer.write_bytes(auth_data, len(auth_data))
        elif (context.server_capabilities & Capabilities.SECURE_CONNECTION) != 0:
            writer.write_byte(len(auth_data))
            writer.write_bytes(auth_data, len(auth_data))
        else:
            writer.write_bytes(auth_data, len(auth_data))
            writer.write_byte(0x00)

        if (self.client_capabilities & Capabilities.CONNECT_WITH_DB) != 0:
            writer.write_string(self.database)
            writer.write_byte(0x00)

        if (context.server_capabilities & Capabilities.PLUGIN_AUTH) != 0:
            writer.write_string(self.authentication_plugin_type)
            writer.write_byte(0x00)

        if (context.server_capabilities & Capabilities.CONNECT_ATTRS) != 0:
            write_connect_attributes(writer, self.connection_attributes, self.host)
        writer.flush()
