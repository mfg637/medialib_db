try:
    import mysql.connector
except ImportError:
    raise Exception("Mysql connector not properly installed")

try:
    import psycopg2
except ImportError:
    raise Exception("Connector psycopq2 not properly installed")

try:
    from .. import config
except ImportError:
    import config

connection = None


def open_connection_if_not_opened():
    global connection
    global db_lock
    if connection is None:
        connection = mysql.connector.connect(
            host=config.db_host, database=config.db_name, user=config.db_user, password=config.db_password
        )


def make_connection():
    return psycopg2.connect(
        host=config.db_host, database=config.db_name, user=config.db_user, password=config.db_password
    )


def mysql_make_connection():
    return mysql.connector.connect(
        host=config.db_host, database=config.db_name, user=config.db_user, password=config.db_password
    )


def close_connection_if_not_closed():
    global connection
    if connection is not None:
        connection.close()
        connection = None


def postgres_string_format(tag_name, size):
    if tag_name is None:
        return None
    elif len(tag_name) > size:
        words = []
        if "_" in tag_name:
            words = tag_name.split("_")
        else:
            words = tag_name.split(" ")
        new_tag_name = ""
        words_copy = 1
        words_count = len(words)
        while len(" ".join(words[:words_copy + 1])+"...") < size \
                and words_copy < words_count:
            words_copy += 1
        new_tag_name = " ".join(words)+"..."
        return new_tag_name[:size]
    return tag_name
