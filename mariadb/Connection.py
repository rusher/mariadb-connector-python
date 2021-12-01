import threading
from threading import RLock

from mariadb.Cursor import Cursor
from mariadb.client.Client import Client
from mariadb.client.Context import Context
from mariadb.message.client.PingPacket import PingPacket
from mariadb.message.client.QueryPacket import QueryPacket
from mariadb.util.ExceptionFactory import ExceptionFactory
from mariadb.util.constant import ConnectionState, ServerStatus


class Connection:
    CALLABLE_STATEMENT_PATTERN = "^(\\s*\\{)?\\s*((\\?\\s*=)?(\\s*/\\*([^*]|\\*[^/])*\\*/)*\\s*call(\\s*/\\*([^*]|\\*[^/])*\\*/)*\\s*((((`[^`]+`)|([^`}]+))\\.)?((`[^`]+`)|([^`}(]+)))\\s*(\\(.*\\))?(\\s*/\\*([^*]|\\*[^/])*\\*/)*\\s*(#.*)?)\\s*(}\\s*)?$"

    def __init__(self, conf, lock: RLock, client: Client):
        self.conf = conf
        self.lock = lock
        self.__client = client

    def cancel_current_query(self):
        cli = Client(self.conf, self.__client.host_address, threading.RLock, True)
        cli.execute(QueryPacket("KILL QUERY " + str(self.__client.context.thread_id)))

    def cursor(self) -> Cursor:
        return Cursor(self.__client, self.lock)

    @property
    def autocommit(self) -> bool:
        return (self.__client.context.server_status & ServerStatus.AUTOCOMMIT) > 0

    @autocommit.setter
    def autocommit(self, auto_commit: bool) -> None:
        if auto_commit == self.autocommit:
            return

        self.lock.acquire()
        try:
            self.__client.context.add_state_flag(ConnectionState.STATE_AUTOCOMMIT)
            self.__client.execute(QueryPacket("set autocommit=1" if auto_commit else "set autocommit=0"))
        finally:
            self.lock.release()

    def commit(self) -> None:
        self.lock.acquire()
        try:
            if (self.__client.context.server_status & ServerStatus.IN_TRANSACTION) > 0:
                self.__client.execute(QueryPacket("COMMIT"))
        finally:
            self.lock.release()

    def rollback(self) -> None:
        self.lock.acquire()
        try:
            if (self.__client.context.server_status & ServerStatus.IN_TRANSACTION) > 0:
                self.__client.execute(QueryPacket("ROLLBACK"))
        finally:
            self.lock.release()

    def close(self) -> None:
        # if (poolConnection != null) {
        # MariaDbPoolConnection poolConnection = this.poolConnection;
        # poolConnection.close();
        # return;

        self.__client.close()

    def __del__(self):
        self.__client.close()

    def is_closed(self) -> bool:
        return self.__client.closed

    def context(self) -> Context:
        return self.__client.context

    def check_not_closed(self) -> None:
        if self.__client.closed:
            raise self.__client.exception_factory.create("Connection is closed", "08000", 1220)

    def is_valid(self) -> bool:
        self.lock.acquire()
        try:
            ping_packet = PingPacket()
            self.__client.execute(ping_packet)
            return True
        except Exception as e:
            return False
        finally:
            self.lock.release()

    @property
    def client(self) -> Client:
        return self.__client

    @property
    def exception_factory(self) -> ExceptionFactory:
        return self.__client.exception_factory

    def thread_id(self) -> int:
        return self.__client.context.thread_id

    def version_greater_or_equal(self, major, minor, patch) -> bool:
        return self.__client.context.version.version_greater_or_equal(major, minor, patch)

    @property
    def mariadb_server(self) -> bool:
        return self.__client.context.version.mariadb_server
