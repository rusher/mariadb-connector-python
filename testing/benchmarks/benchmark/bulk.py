#!/usr/bin/env python3 -O
# -*- coding: utf-8 -*-

import pyperf


def bulk(loops, conn):
    cursor = conn.cursor()
    cursor.execute("CREATE TEMPORARY TABLE test_update_bulk (a int primary key, b int)")
#    conn.autocommit= False
    t0 = pyperf.perf_counter()
    vals = [(i,) for i in range(10000)]
    range_it = range(loops)
    for value in range_it:
        cursor.executemany("INSERT INTO test_update_bulk VALUES (?, NULL)", vals);
        conn.commit()
        cursor.executemany("UPDATE test_update_bulk SET b=2 WHERE a=?", vals);
        conn.commit()
        cursor.executemany("DELETE FROM test_update_bulk WHERE a=?", vals);
        conn.commit()
        cursor.execute('do 1')
    cursor.execute("DROP TABLE IF EXISTS test_update_bulk")
    del cursor
    return pyperf.perf_counter() - t0
