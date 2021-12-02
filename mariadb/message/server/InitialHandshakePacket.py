from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.message.server.util.ServerVersionUtility import ServerVersionUtility
from mariadb.util.constant import Capabilities

MARIADB_RPL_HACK_PREFIX = "5.5.5-"


class InitialHandshakePacket:

    def __init__(self, server_version: str, thread_id: int, seed: bytearray, capabilities: int, default_collation: str,
                 server_status: int, mariadb_server: bool, authentication_plugin_type: str):
        self.thread_id = thread_id
        self.seed = seed
        self.capabilities = capabilities
        self.default_collation = default_collation
        self.server_status = server_status
        self.authentication_plugin_type = authentication_plugin_type
        self.version = ServerVersionUtility(server_version, mariadb_server)

    @staticmethod
    def decode(reader: ReadableByteBuf):
        protocol_version = reader.read_byte()
        if protocol_version != 0x0a:
            raise Exception("Unexpected initial handshake protocol value [{}]".format(protocol_version))

        server_version = reader.read_string_null_end()
        thread_id = reader.read_int()
        seed1 = bytearray(8)
        reader.read_bytes(seed1)
        reader.skip_one()
        server_capabilities_two_first_bytes = reader.read_unsigned_short()

        default_collation = reader.read_unsigned_byte()
        server_status = reader.read_short()
        server_capabilities_four_first_bytes = server_capabilities_two_first_bytes + (reader.read_short() << 16)
        salt_length = 0

        if (server_capabilities_four_first_bytes & Capabilities.PLUGIN_AUTH) != 0:
            salt_length = max(12, reader.read_byte() - 9)
        else:
            reader.skip_one()

        reader.skip(6)

        # MariaDB additional capabilities.
        # Filled only if MariaDB server 10.2+
        mariadb_additional_capacities = reader.read_int()

        if (server_capabilities_four_first_bytes & Capabilities.SECURE_CONNECTION) != 0:
            if salt_length > 0:
                seed2 = bytearray(salt_length)
                reader.read_bytes(seed2)
            else:
                seed2 = reader.read_bytes_null_end()
            seed = bytearray(len(seed1) + len(seed2))
            seed[0:len(seed1)] = seed1
            seed[len(seed1):len(seed2)] = seed2
        else:
            seed = seed1

        reader.skip_one()

        # check for MariaDB 10.x replication hack , remove fake prefix if needed
        #  (see comments about MARIADB_RPL_HACK_PREFIX)
        if server_version.startswith(MARIADB_RPL_HACK_PREFIX):
            server_mariadb = True
            server_version = server_version[len(MARIADB_RPL_HACK_PREFIX):]
        else:
            server_mariadb = "MariaDB" in server_version

        # since MariaDB 10.2
        if (server_capabilities_four_first_bytes & Capabilities.CLIENT_MYSQL) == 0:
            server_capabilities = (server_capabilities_four_first_bytes & 0xffffffff) + (
                    mariadb_additional_capacities << 32)
            server_mariadb = True
        else:
            server_capabilities = server_capabilities_four_first_bytes & 0xffffffff

        authentication_plugin_type = None
        if (server_capabilities_four_first_bytes & Capabilities.PLUGIN_AUTH) != 0:
            authentication_plugin_type = reader.read_string_null_end()

        return InitialHandshakePacket(
            server_version,
            thread_id,
            seed,
            server_capabilities,
            default_collation,
            server_status,
            server_mariadb,
            authentication_plugin_type)

    def streaming(self) -> bool:
        return False
