#!/usr/bin/env python -O
# -*- coding: utf-8 -*-
import logging
import logging.config
import mariadb

from .conf_test import conf
from os import path


def is_skysql():
    if conf()["host"][-13:] == "db.skysql.net":
        return True
    return False


def create_connection(additional_conf=None):
    log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
    logging.config.fileConfig(log_file_path)
    default_conf = conf()
    if additional_conf is None:
        c = {key: value for (key, value) in (default_conf.items())}
    else:
        c = {key: value for (key, value) in (list(default_conf.items()) + list(
            additional_conf.items()))}
    return mariadb.connect(**c)
