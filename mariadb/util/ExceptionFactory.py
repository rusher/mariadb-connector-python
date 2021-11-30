class SQLError(Exception):
    """Exception related to operation with database."""


class SQLTimeoutException(SQLError):
    """Exception occurring when timeout occurs."""


class SQLFeatureNotSupportedException(SQLError):
    """Feature is not supported"""


class SQLSyntaxErrorException(SQLError):
    """SQL syntax not supported"""


class SQLInvalidAuthorizationSpecException(SQLError):
    """SQL invalid"""


class SQLIntegrityConstraintViolationException(SQLError):
    """Constraint violation"""


class SQLNonTransientConnectionException(SQLError):
    """Connection error"""


class SQLTransientConnectionException(SQLError):
    """Connection error"""


class MaxAllowedPacketException(SQLError):
    """packet reach maximum size"""


class ExceptionFactory:

    def __init__(self, conf, host_address, connection=None, pool_connection=None, thead_id=None, cursor=None,
                 sql=None):
        self.__conf = conf
        self.__host_address = host_address
        self.__connection = connection
        self.__pool_connection = pool_connection
        self.__thead_id = thead_id
        self.__cursor = cursor
        self.__sql = sql

    # @staticmethod
    # def
    # private ExceptionFactory(
    # Connection connection,
    # MariaDbPoolConnection poolConnection,
    # Configuration conf,
    # HostAddress hostAddress,
    # long threadId,
    # cursor cursor) {
    # this.connection = connection;
    # this.poolConnection = poolConnection;
    # this.conf = conf;
    # this.hostAddress = hostAddress;
    # this.threadId = threadId;
    # this.cursor = cursor;
    # }

    @staticmethod
    def build_msg_text(initial_message, thread_id, conf, sql, error_code, connection):

        msg = ""
        if thread_id != 0:
            msg += "(conn=" + str(thread_id) + ") "

        msg += initial_message

        if conf.get('dump_queries_on_exception') and sql is not None:
            if conf.get('max_query_size_to_log') != 0 and sql.length() > conf.get('max_query_size_to_log') - 3:
                msg += "\nQuery is: " + sql[0: conf.get('max_query_size_to_log') - 3] + "..."
            else:
                msg += "\nQuery is: " + sql

        if conf.get('show_innodb_dead_lock') and error_code in [1205, 1213, 1614] and connection is not None:
            stmt = connection.createcursor()
            try:
                rs = stmt.executeQuery("SHOW ENGINE INNODB STATUS")
                rs.next()
                msg += "\ndeadlock information: " + rs.getString(3)
            except Exception:
                # eat exception
                pass

        return msg

    @property
    def connection(self):
        return self.__connection

    @connection.setter
    def connection(self, value):
        self.__connection = value

    @property
    def pool_connection(self):
        return self.__pool_connection

    @pool_connection.setter
    def pool_connection(self, value):
        self.__pool_connection = value

    @property
    def thead_id(self):
        return self.__thead_id

    @thead_id.setter
    def thead_id(self, value):
        self.__thead_id = value

    @property
    def sql(self):
        return self.__sql

    def of_stmt(self, cursor):
        return ExceptionFactory(self.__conf, self.__host_address, self.__connection, self.__pool_connection,
                                self.__thead_id,
                                cursor)

    def with_sql(self, sql):
        return ExceptionFactory(self.__conf, self.__host_address, self.__connection, self.__pool_connection,
                                self.__thead_id,
                                self.__cursor, sql)

    def _create_exception(self, initial_message, sql_state="42000", error_code=-1, cause=None):
        msg = ExceptionFactory.build_msg_text(initial_message, self.__thead_id, self.__conf, self.__sql, error_code,
                                              self.__connection)

        if "70100" == sql_state:
            # ER_QUERY_INTERRUPTED
            return SQLTimeoutException(msg, sql_state, error_code)

        sql_class = "42" if sql_state is None else sql_state[0:2]
        switcher = {
            "0A": SQLFeatureNotSupportedException(msg, sql_state, error_code, cause),
            "22": SQLSyntaxErrorException(msg, sql_state, error_code, cause),
            "26": SQLSyntaxErrorException(msg, sql_state, error_code, cause),
            "2F": SQLSyntaxErrorException(msg, sql_state, error_code, cause),
            "20": SQLSyntaxErrorException(msg, sql_state, error_code, cause),
            "42": SQLSyntaxErrorException(msg, sql_state, error_code, cause),
            "XA": SQLSyntaxErrorException(msg, sql_state, error_code, cause),
            "25": SQLInvalidAuthorizationSpecException(msg, sql_state, error_code, cause),
            "28": SQLInvalidAuthorizationSpecException(msg, sql_state, error_code, cause),
            "21": SQLIntegrityConstraintViolationException(msg, sql_state, error_code, cause),
            "23": SQLIntegrityConstraintViolationException(msg, sql_state, error_code, cause),
            "08": SQLNonTransientConnectionException(msg, sql_state, error_code, cause),
            "HY": SQLError(msg, sql_state, error_code, cause)
        }
        return_ex = switcher.get(sql_class, lambda: SQLTransientConnectionException(msg, sql_state, error_code, cause))

        # if self.__pool_connection is not None and isinstance(self.__cursor, Preparedcursor):
        #     self.__pool_connection.firecursorErrorOccurred(self.__cursor, return_ex)
        #
        # if isinstance(return_ex, SQLNonTransientConnectionException) or isinstance(return_ex,
        #                                                                            SQLTransientConnectionException):
        #     self.__pool_connection.fireConnectionErrorOccurred(return_ex)
        return return_ex

    def not_supported(self, message):
        return self._create_exception(message, "0A000")

    def create(self, message, sql_state):
        return self._create_exception(message, sql_state)

    def create(self, message, sql_state="42000", error_code=-1, cause=None):
        return self._create_exception(message, sql_state, error_code, cause)
