try:
    import mysql.connector
except ImportError:
    raise Exception("Mysql connector not properly installed")

import multiprocessing
try:
    from .. import config
except ImportError:
    import config

connection = None

#db_lock = multiprocessing.Lock()


def open_connection_if_not_opened():
    global connection
    global db_lock
    if connection is None:
        #db_lock.acquire()
        connection = mysql.connector.connect(
            host=config.db_host, database=config.db_name, user=config.db_user, password=config.db_password
        )


def make_connection():
    return mysql.connector.connect(
        host=config.db_host, database=config.db_name, user=config.db_user, password=config.db_password
    )


def close_connection_if_not_closed():
    global connection
    global db_lock
    if connection is not None:
        #db_lock.release()
        connection.close()
        connection = None


def db_lock_init(lock_):
    global db_lock
    db_lock = lock_
