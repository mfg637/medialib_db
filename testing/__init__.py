try:
    import mysql.connector
except ImportError:
    raise Exception("Mysql connector not properly installed")

try:
    from .. import config
except ImportError:
    import config


def make_connection():
    return mysql.connector.connect(
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
        "ALTER TABLE content AUTO_INCREMENT = 0",
        "ALTER TABLE tag_alias AUTO_INCREMENT = 0",
        "ALTER TABLE tag AUTO_INCREMENT = 0"
    ]
    cursor = connection.cursor()
    for sql_resuest in sql_resuests:
        cursor.execute(sql_resuest)
    connection.commit()
    connection.close()
