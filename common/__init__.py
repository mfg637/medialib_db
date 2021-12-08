import mysql.connector
try:
    from .. import config
except ImportError:
    import config

connection = None


def open_connection_if_not_opened():
    global connection
    if connection is None:
        connection = mysql.connector.connect(
            host=config.db_host, database=config.db_name, user=config.db_user, password=config.db_password
        )


def close_connection_if_not_closed():
    global connection
    if connection is not None:
        connection.close()
        connection = None
