#!/usr/bin/env python3 -O
# -*- coding: utf-8 -*-

import pyperf, os

from benchmarks.benchmark.do_1 import do1
from benchmarks.benchmark.fetch import *

from benchmarks.benchmark.select_1 import select_1
from benchmarks.benchmark.select_param import select_param
from benchmarks.benchmark.select_10_cols_from_seq_1_to_10000 import \
    select_10_cols_from_seq_1_to_10000
from benchmarks.benchmark.select_1_mysql_user import select_1_mysql_user
from benchmarks.benchmark.bulk import bulk

def run_test(tests, conn, paramstyle):
    runner = pyperf.Runner()
    for test in tests:
        runner.bench_time_func(test['label'], test['method'], conn, paramstyle)


def test_suite(paramstyle):
    is_mysql= int(os.environ.get('TEST_MYSQL', '1'))
    ts= [
        {'label': '100 rows * 3 col utf8 string using fetchone', 'method': str_fetchloop},
        {'label': '100 rows * 3 col utf8 string using fetchall', 'method': str_fetchall},
        {'label': '1000 rows * 5 numeric col using fetchone', 'method': num_fetchloop},
        {'label': '1000 rows * 5 numeric col using fetchall', 'method': num_fetchall},
        {'label': 'select 1', 'method': select_1},
        {'label': 'select ? - param 1', 'method': select_param},
        {'label': 'bulk: insert/update/delete', 'method': bulk},
    ]
    if is_mysql == 1:
        ts.append({'label': 'select 1 mysql user', 'method': select_1_mysql_user})
        ts.append({'label': 'do 1', 'method': do1})
        ts.append({'label': 'Select <10 cols of 100 chars> from_seq_1_to_100000', 'method':
            select_10_cols_from_seq_1_to_10000})
    return ts
