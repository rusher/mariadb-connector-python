import threading

from mariadb.Connection import Connection
from mariadb.HostAddress import HostAddress
from mariadb.client.Client import Client

threadsafety = 1
apilevel = "2.0"
paramstyle = "qmark"


def connect(**arg) -> Connection:
    conf = dict(arg)

    conf.setdefault("host", "localhost")
    conf.setdefault("port", 3306)
    conf.setdefault("non_mapped_options", {})
    conf.setdefault("socket_timeout", 30)
    conf.setdefault("pipe")
    conf.setdefault("local_socket")
    conf.setdefault("local_socket_address")
    conf.setdefault("tcp_keep_alive", False)
    conf.setdefault("tcp_abortive_close", False)
    conf.setdefault("max_query_size_to_log", 1024)
    conf.setdefault("use_binary", True)
    conf.setdefault("use_bulk", True)
    conf.setdefault("use_affected_rows", False)
    conf.setdefault("allow_multi_queries", False)
    conf.setdefault("allow_local_infile", False)
    conf.setdefault("use_compression", False)
    conf.setdefault("dump_queries_on_exception", False)
    conf.setdefault("show_innodb_dead_lock", False)

    conf.setdefault("database")
    conf.setdefault("prep_stmt_cache_size", 250)
    conf.setdefault("user")
    conf.setdefault("password")
    conf.setdefault("connection_attributes")

    host_address = HostAddress(conf.get("host"), conf.get("port", 3306))
    lock = threading.RLock()
    client = Client(conf, host_address, lock)
    return Connection(conf, lock, client)
