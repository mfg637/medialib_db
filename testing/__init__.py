try:
    import psycopg2
except ImportError:
    raise Exception("Connector psycopq2 not properly installed")

try:
    from .. import config
except ImportError:
    import config


def make_connection():
    return psycopg2.connect(
        host=config.db_host, database=config.db_test, user=config.db_user, password=config.db_password
    )


def wipe():
    connection = make_connection()
    sql_resuests = [
        "DELETE FROM content_tags_list",
        "DELETE FROM thumbnail",
        "DELETE FROM tag_alias",
        "DELETE FROM content",
        "DELETE FROM tag",
        "ALTER SEQUENCE content_id_seq RESTART WITH 1",
        "ALTER SEQUENCE tag_id_seq RESTART WITH 1"
    ]
    cursor = connection.cursor()
    for sql_resuest in sql_resuests:
        cursor.execute(sql_resuest)
    connection.commit()
    connection.close()
