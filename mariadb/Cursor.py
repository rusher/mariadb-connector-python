from threading import RLock

from mariadb.client.Client import Client
from mariadb.client.result.Result import Result
from mariadb.message.client.BulkExecutePacket import BulkExecutePacket
from mariadb.message.client.ExecutePacket import ExecutePacket
from mariadb.message.client.PreparePacket import PreparePacket
from mariadb.message.client.QueryPacket import QueryPacket
from mariadb.message.client.QueryWithParametersPacket import QueryWithParametersPacket
from mariadb.message.server.OkPacket import OkPacket
from mariadb.util.ClientParser import parameter_parts
from mariadb.util.ExceptionFactory import ExceptionFactory
from mariadb.util.constant import ServerStatus, Capabilities


class Cursor:

    def __init__(self, client: Client, lock: RLock):
        self.__client = client
        self.__lock = lock
        self.__closed = False
        self.__curr_result = None
        self.__results = None
        self.__arraysize = 1
        self.__execute_stmt_with_param = self.__execute_binary_stmt_with_param if client.conf.get(
            "use_binary") else self.__execute_text_stmt_with_param

        self.__executemany = self.__executemany_binary if client.conf.get(
            "use_binary") else self.__executemany_text

    def __exception_factory(self) -> ExceptionFactory:
        return self.__client.exception_factory.of_stmt(self)

    def execute(self, sql: str, parameters=None) -> None:
        if parameters is None:
            self.__execute_stmt(sql)
        else:
            self.__execute_stmt_with_param(sql, parameters)
        self.__curr_result = self.__results.pop(0)

    def close(self):
        if not self.__closed:
            self.__closed = True
            if self.__curr_result is not None and isinstance(self.__curr_result, Result):
                self.__curr_result.close_from_stmt_close(self.__lock)

            # close result-set
            if self.__results is not None:
                for completion in self.__results:
                    if isinstance(completion, Result):
                        completion.close_from_stmt_close(self.__lock)

    def __del__(self):
        self.close()

    def abort(self) -> None:
        self.__lock.acquire()
        try:
            if not self.__closed:
                self.__closed = True
            if self.__curr_result is not None and isinstance(self.__curr_result, Result):
                self.__curr_result.abort()

            # close result-set
            if self.__results is not None:
                for completion in self.__results:
                    isinstance(completion, Result)
                    completion.abort()
        except Exception:
            pass
        finally:
            self.__lock.release()

    def fetchone(self) -> tuple:
        if isinstance(self.__curr_result, Result):
            return self.__curr_result.fetchone()
        return None

    def fetchall(self) -> tuple:
        if isinstance(self.__curr_result, Result):
            return self.__curr_result.fetchall()
        return None

    @property
    def rowcount(self) -> int:
        if isinstance(self.__curr_result, OkPacket):
            return self.__curr_result.affected_rows
        return -1

    def nextset(self) -> bool:
        if self.__curr_result is None:
            raise Exception("must execute some command before .nextset()")
        if isinstance(self.__curr_result, Result):
            self.__lock.acquire()
            try:
                self.__curr_result.close()
                if self.__curr_result.streaming() and (
                        self.__client.context.server_status & ServerStatus.MORE_RESULTS_EXISTS) > 0:
                    self.__client.read_streaming_results(self.__results, self.__arraysize)
            finally:
                self.__lock.release()
        while len(self.__results) > 0:
            self.__curr_result = self.__results.pop(0)
            if isinstance(self.__curr_result, Result):
                return True
        self.__curr_result = None
        return None

    @property
    def arraysize(self) -> int:
        return self.__arraysize

    @arraysize.setter
    def arraysize(self, arraysize: int):
        if arraysize < 0:
            raise self.__exception_factory().create("invalid fetch size")

        self.__arraysize = arraysize

    def executemany(self, sql: str, batch_parameters):
        self.check_not_closed()
        has_param = False
        if len(batch_parameters) > 0:
            if len(batch_parameters[0]) > 0:
                has_param = True

        self.__results = []
        self.__curr_result = None
        self.__lock.acquire()
        try:
            if not has_param:
                for param in batch_parameters:
                    self.__results.extend(self.__client.execute(QueryPacket(sql), self, self.__arraysize))
            else:
                self.__executemany(sql, batch_parameters)
            self.__curr_result = self.__results.pop(0)
        finally:
            self.__lock.release()

    def __executemany_text(self, sql: str, batch_parameters) -> None:
        no_backslash_escapes = (self.__client.context.server_status & ServerStatus.NO_BACKSLASH_ESCAPES) > 0
        parser = parameter_parts(sql, no_backslash_escapes)
        for param in batch_parameters:
            if len(param) < parser.param_count:
                raise Exception('some parameters are not set')
            self.__results = self.__client.execute(QueryWithParametersPacket(parser, param), self,
                                                   self.__arraysize)

    def __executemany_binary(self, sql: str, batch_parameters) -> None:
        self.prepare = self.__client.context.prepare_cache.get(sql)
        msgs = []
        if (self.__client.context.server_capabilities & Capabilities.MARIADB_CLIENT_STMT_BULK_OPERATIONS) > 0:
            if self.__client.conf.get("use_bulk"):
                if not self.prepare:
                    msgs.append(PreparePacket(sql, self.__client))
                    statement_id = -1
                else:
                    statement_id = self.prepare.statement_id
                msgs.append(BulkExecutePacket(statement_id, batch_parameters, sql))
                self.__results = self.__client.execute_pipeline(msgs, self, self.__arraysize)

                # remove prepare result
                if statement_id == -1:
                    self.__results.pop(0)
            else:
                # bulk disable, use pipelining
                if not self.prepare:
                    msgs.append(PreparePacket(sql, self.__client))
                    statement_id = -1
                else:
                    statement_id = self.prepare.statement_id

                for params in batch_parameters:
                    msgs.append(ExecutePacket(statement_id, params, sql))
                self.__results = self.__client.execute_pipeline(msgs, self, self.__arraysize)

                # remove prepare result
                if statement_id == -1:
                    self.__results.pop(0)
        else:
            # pipelining not possible, just loop

            if not self.prepare:
                self.prepare = self.__client.execute(PreparePacket(sql, self.__client), self, self.__arraysize)[0]
            statement_id = self.prepare.statement_id

            res = []
            for params in batch_parameters:
                res.append(self.__client.execute(ExecutePacket(statement_id, params, sql), self, self.__arraysize))
            self.__results = res

    def setinputsizes(self, sizes) -> None:
        pass

    def setoutputsize(self, size, column=None) -> None:
        pass

    def __execute_stmt(self, sql: str) -> None:
        self.check_not_closed()
        self.__lock.acquire()
        try:
            self.__results = self.__client.execute(QueryPacket(sql), self, self.__arraysize)
        finally:
            self.__lock.release()

    def __execute_text_stmt_with_param(self, sql: str, parameters) -> None:
        self.check_not_closed()
        no_backslash_escapes = (self.__client.context.server_status & ServerStatus.NO_BACKSLASH_ESCAPES) > 0
        parser = parameter_parts(sql, no_backslash_escapes)
        self.__lock.acquire()
        try:
            params = parameters
            if type(parameters) != tuple:
                params = tuple(parameters)
            if len(params) < parser.param_count:
                raise Exception('some parameters are not set')
            self.__results = self.__client.execute(QueryWithParametersPacket(parser, params), self, self.__arraysize)
        finally:
            self.__lock.release()

    def __execute_binary_stmt_with_param(self, sql: str, parameters) -> None:
        self.check_not_closed()
        no_backslash_escapes = (self.__client.context.server_status & ServerStatus.NO_BACKSLASH_ESCAPES) > 0
        parser = parameter_parts(sql, no_backslash_escapes)
        self.__lock.acquire()
        try:
            params = parameters
            if type(parameters) != tuple:
                params = tuple(parameters)
            if len(params) < parser.param_count:
                raise Exception('some parameters are not set')
            self.prepare = self.__client.context.prepare_cache.get(sql)
            if self.prepare:
                self.__results = self.__client.execute(ExecutePacket(self.prepare.statement_id, params, sql), self,
                                                       self.__arraysize)
            elif (self.__client.context.server_capabilities & Capabilities.MARIADB_CLIENT_STMT_BULK_OPERATIONS) > 0:
                # pipelining only for MariaDB servers
                msgs = [
                    PreparePacket(sql, self.__client),
                    ExecutePacket(-1, params, sql)
                ]
                self.__results = self.__client.execute_pipeline(msgs, self, self.__arraysize)
            else:
                self.prepare = self.__client.execute(PreparePacket(sql, self.__client), self, self.__arraysize)[0]
                self.__results = self.__client.execute(ExecutePacket(self.prepare, params, sql), self, self.__arraysize)
        finally:
            self.__lock.release()

    def check_not_closed(self) -> None:
        if self.__closed:
            raise self.__client.exception_factory.create("Connection is closed", "08000", 1220)

    def update_meta(self, ci) -> None:
        self.prepare.columns = ci
