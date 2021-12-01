from ..message.server.InitialHandshakePacket import InitialHandshakePacket
from ..util.ExceptionFactory import ExceptionFactory
from ..util.constant import Capabilities


class Context:

    __slots__ = (
        'thread_id', 'seed', 'server_capabilities', 'server_status', 'version', 'eof_deprecated', 'skip_meta', 'extended_info',
        'conf', 'state_flag', 'database', 'exception_factory', 'transaction_isolation_level', 'prepare_cache')
    
    def __init__(self, handshake: InitialHandshakePacket, client_capabilities: int, conf,
                 exception_factory: ExceptionFactory, prepare_cache):
        self.thread_id = handshake.thread_id
        self.seed = handshake.seed
        self.server_capabilities = handshake.capabilities
        self.server_status = handshake.server_status
        self.version = handshake.version
        self.eof_deprecated = (client_capabilities & Capabilities.CLIENT_DEPRECATE_EOF) > 0
        self.skip_meta = (client_capabilities & Capabilities.MARIADB_CLIENT_CACHE_METADATA) > 0
        self.extended_info = (self.server_capabilities & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO) > 0
        self.conf = conf
        self.state_flag = 0
        self.database = conf.get('database')
        self.exception_factory = exception_factory
        self.transaction_isolation_level = None
        self.prepare_cache = prepare_cache
        # self.prepare_cache = prepare_cache

    def reset_prepare_cache(self):
        self.prepare_cache.reset()

    def reset_state_flag(self):
        self.state_flag = 0

    def add_state_flag(self, state: int):
        self.state_flag |= state
