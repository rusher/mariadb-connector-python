#!/usr/bin/env python -O
# -*- coding: utf-8 -*-
import logging
import os
import unittest

import pymysql

import mariadb

# from test.base_test import create_connection, is_skysql
# from test.conf_test import conf
import platform

from testing.test.base_test import create_connection
from testing.test.conf_test import conf

request = "SELECT "

for i in range(0, 10):
    if i == 0:
        request += " REPEAT('a', 100)"
    else:
        request += ", REPEAT('a', 100)"

request += " from seq_1_to_10000 where 1 = ?"

class TestConnection(unittest.TestCase):

    def setUp(self):
        self.connection = create_connection()

    def tearDown(self):
        del self.connection

    def test_conpy36(self):
        if platform.system() == "Windows":
            self.skipTest("unix_socket not supported on Windows")
        default_conf = conf()
        try:
           conn= mariadb.connect(user=default_conf["user"], unix_socket="/does_not_exist/x.sock", port=default_conf["port"], host=default_conf["host"])
        except (mariadb.OperationalError,):
           pass

    def test_connection_default_file(self):
        if os.path.exists("client.cnf"):
            os.remove("client.cnf")
        default_conf = conf()
        f = open("client.cnf", "w+")
        f.write("[client]\n")
        f.write("host=%s\n" % default_conf["host"])
        f.write("port=%i\n" % default_conf["port"])
        f.write("user=%s\n" % default_conf["user"])
        if "password" in default_conf:
            f.write("password=%s\n" % default_conf["password"])
        f.write("database=%s\n" % default_conf["database"])
        f.close()

        new_conn = mariadb.connect(user=default_conf["user"], ssl=True, default_file="./client.cnf")
        self.assertEqual(new_conn.database, default_conf["database"])
        del new_conn
        os.remove("client.cnf")

    def test_autocommit(self):
        conn = self.connection
        conn.autocommit = False
        self.assertEqual(conn.autocommit, False)
        # revert
        conn.autocommit = True
        self.assertEqual(conn.autocommit, True)

    def test_local_infile(self):
        default_conf= conf()
        new_conn = mariadb.connect(**default_conf, local_infile=False)
        cursor=new_conn.cursor()
        cursor.execute("CREATE TEMPORARY TABLE t1 (a int)")
        try:
            cursor.execute("LOAD DATA LOCAL INFILE 'x.x' INTO TABLE t1")
        except (mariadb.OperationalError,):
            pass
        del cursor
        del new_conn

    def test_init_command(self):
        default_conf= conf()
        new_conn = mariadb.connect(**default_conf, init_command="SET @a:=1")
#, port=default_conf["port"], host=default_conf["host"], password=default_conf["password"])
        cursor=new_conn.cursor()
        cursor.execute("SELECT @a")
        row=cursor.fetchone()
        self.assertEqual(row[0], 1)
        del cursor
        del new_conn

    def test_compress(self):
        default_conf= conf()
        new_conn = mariadb.connect(**default_conf, compress=True)
        cursor=new_conn.cursor()
        cursor.execute("SHOW SESSION STATUS LIKE 'compression'")
        row=cursor.fetchone()
        if os.environ.get("MAXSCALE_VERSION"):
            self.assertEqual(row[1], "OFF")
        else:
            self.assertEqual(row[1], "ON")
        del cursor
        del new_conn

    def test_schema(self):
        if self.connection.server_version < 100103:
            self.skipTest("CREATE OR REPLACE SCHEMA not supported")
        if self.connection.server_version < 100202:
            self.skipTest("session tracking not supported")
        if os.environ.get("MAXSCALE_VERSION"):
            self.skipTest("MAXSCALE doesn't tell schema change for now")

        default_conf = conf()
        conn = self.connection
        self.assertEqual(conn.database, default_conf["database"])
        cursor = conn.cursor()
        cursor.execute("CREATE OR REPLACE SCHEMA test1")
        cursor.execute("USE test1")
        self.assertEqual(conn.database, "test1")
        conn.database = default_conf["database"]
        self.assertEqual(conn.database, default_conf["database"])

    def test_ping(self):
        if os.environ.get("MAXSCALE_VERSION"):
            self.skipTest("MAXSCALE wrong thread id")
        conn = self.connection
        cursor = conn.cursor()
        oldid = conn.connection_id

        try:
            cursor.execute("KILL {id}".format(id=oldid))
        except:
            pass

        conn.auto_reconnect = True
        conn.ping()
        self.assertNotEqual(oldid, conn.connection_id)
        self.assertNotEqual(0, conn.connection_id)

    def test_ed25519(self):
        if is_skysql():
            self.skipTest("Test fail on SkySQL")
        default_conf = conf()
        if os.environ.get("MAXSCALE_VERSION"):
            self.skipTest("MAXSCALE doesn't support ed25519 for now")
        if self.connection.server_version < 100122:
            self.skipTest("ed25519 not supported")

        conn = self.connection
        curs = conn.cursor(buffered=True)

        if self.connection.server_name == "localhost":
          curs.execute("select * from information_schema.plugins where plugin_name='unix_socket' and plugin_status='ACTIVE'")
          if curs.rowcount > 0:
              del curs
              self.skipTest("unix_socket is active")

        cursor = conn.cursor()
        try:
            cursor.execute("INSTALL SONAME 'auth_ed25519'")
        except (mariadb.DatabaseError, mariadb.OperationalError):
            self.skipTest("Server couldn't load auth_ed25519")
        cursor.execute("DROP USER IF EXISTS eduser")
        if self.connection.server_version < 100400:
            cursor.execute("CREATE USER eduser@'%' IDENTIFIED VIA ed25519 "
                           "USING '6aW9C7ENlasUfymtfMvMZZtnkCVlcb1ssxOLJ0kj/AA'")
        else:
            cursor.execute("CREATE USER eduser@'%' IDENTIFIED VIA ed25519 "
                           "USING PASSWORD('MySup8%rPassw@ord')")
        cursor.execute("GRANT ALL on " + default_conf["database"] + ".* to eduser@'%'")
        conn2 = create_connection({"user": "eduser", "password":
            "MySup8%rPassw@ord"})
        cursor.execute("DROP USER IF EXISTS eduser")
        try:
            create_connection({"user": "eduser", "password": "MySup8%rPassw@ord", "plugin_dir": "wrong_plugin_dir"})
            self.fail("wrong plugin directory, must not have found authentication plugin")
        except (mariadb.OperationalError):
            pass
        cursor.execute("DROP USER IF EXISTS eduser")
        del cursor, conn2

    def test_conpy46(self):
        with create_connection() as con:
            with con.cursor() as cursor:
                cursor.execute("SELECT 'foo'")
                row= cursor.fetchone()
                self.assertEqual(row[0], "foo")
            try:
                cursor.execute("SELECT 'bar'")
            except mariadb.ProgrammingError:
                pass
        try:
            cursor= con.cursor()
        except mariadb.ProgrammingError:
            pass

    def test_conpy101(self):
        default_conf = conf()
        conn = mariadb.connect(**default_conf)

        cursor=conn.cursor()

        for value in range(10000):
            cursor.execute("select ?", (1,))
            row = cursor.fetchall()
            del row
        del cursor
        del conn

    def test_conpy155(self):
        default_conf= conf()
        c1 = mariadb.connect(**default_conf)
        cursor = c1.cursor()
        for value in range(10):
            cursor.execute('select col1,col2,col3 from str_test where 1 = ?', (1,))
        row= cursor.fetchone()
        while row is not None:
            row= cursor.fetchone()
        del cursor


        version= c1.get_server_version()
        self.assertEqual(c1.get_server_version(), version)
        self.assertEqual(c1.get_server_version(), version)
        self.assertEqual(c1.get_server_version(), version)
        self.assertEqual(c1.get_server_version(), version)
        c1.close()
        try:
            version= c1.get_server_version()
        except mariadb.InterfaceError:
            pass

    def test_conpy175(self):
        default_conf= conf()
        c1 = mariadb.connect(**default_conf)
        str= '"' * 4194304
        newstr= c1.escape_string(str);
        self.assertEqual(newstr, '\\"' * 4194304)
        c1.close()


if __name__ == '__main__':
    unittest.main()
