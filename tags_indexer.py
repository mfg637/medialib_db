import pathlib
import datetime
import re

import mysql.connector.errors

try:
    from . import config
except ImportError:
    import config

try:
    from . import common
except ImportError:
    import common


def _request(request_body, *args, auto_open_connection=True):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    elif common.connection is None:
        raise OSError("connection is closed")
    cursor = common.connection.cursor()

    result = request_body(cursor, *args)

    if auto_open_connection:
        common.connection.commit()
        common.close_connection_if_not_closed()

    return result


def _check_tag_exists(cursor, tag_name, tag_category):
    sql_select_tag_query = "SELECT ID FROM `tag` WHERE title = %s and category = %s"
    cursor.execute(sql_select_tag_query, (tag_name, tag_category))
    # returns ID of tag
    return cursor.fetchone()


def check_tag_exists(tag_name, tag_category, auto_open_connection=True) -> tuple[int,]:
    """
    Check exists tag in specified category and returns tag ID.
    :rtype: tuple with ID of tag (tuple(int,))
    """
    return _request(_check_tag_exists, tag_name, tag_category, auto_open_connection=auto_open_connection)


def _insert_new_tag(cursor, tag_name, tag_category, tag_alias):
    sql_insert_tag_query = "INSERT INTO `tag` (`ID`, title, category) VALUES (NULL, %s, %s)"
    cursor.execute(sql_insert_tag_query, (tag_name, tag_category))
    tag_id = cursor.lastrowid
    sql_insert_alias_query = "INSERT INTO `tag_alias` (`ID`, tag_id, title) VALUES (NULL, %s, %s)"
    try:
        cursor.execute(sql_insert_alias_query, (tag_id, tag_alias))
    except mysql.connector.errors.IntegrityError as e:
        common.connection.rollback()
        get_tag_info = "SELECT tag.category, ID from tag where title=%s"
        cursor.execute(get_tag_info, (tag_name,))
        response = cursor.fetchone()
        _category = response[0]
        tag_id = response[1]
        if _category == "content":
            update_category = "UPDATE tag Set category=%s where ID=%s"
            cursor.execute(update_category, (tag_category, tag_id))
            return tag_id
        else:
            raise e
    if " " in tag_alias:
        cursor.execute(sql_insert_alias_query, (tag_id, tag_alias.replace(" ", "_")))
    elif "_" in tag_alias:
        cursor.execute(sql_insert_alias_query, (tag_id, tag_alias.replace("_", " ")))
    return tag_id


def insert_new_tag(tag_name, tag_category, tag_alias, auto_open_connection=True) -> int:
    """
    Insert new tag in database's table and returns tag ID.
    :rtype: ID of tag (int)
    """
    return _request(_insert_new_tag, tag_name, tag_category, tag_alias, auto_open_connection=auto_open_connection)


def _tag_register(cursor, tag_name, tag_category, tag_alias):
    tag_id = check_tag_exists(tag_name, tag_category, auto_open_connection=False)
    if tag_id is None:
        tag_id = insert_new_tag(tag_name, tag_category, tag_alias, auto_open_connection=False)
    else:
        tag_id = tag_id[0]
    return tag_id


def tag_register(tag_name, tag_category, tag_alias, auto_open_connection=True):
    """
    Verify tag existing, insert< if not exists, and returns tag ID.
    :rtype: ID of tag (int)
    """
    return _request(_tag_register, tag_name, tag_category, tag_alias, auto_open_connection=auto_open_connection)


def _get_category_of_tag(cursor, tag_name):
    def mysql_escafe_quotes(_string):
        return re.sub("\"", "\\\"", _string)

    get_tag_category_query = "SELECT category FROM tag WHERE title=%s;"
    cursor.execute(get_tag_category_query, (tag_name,))
    return cursor.fetchone()


def get_category_of_tag(tag_name, auto_open_connection=True):
    """
    Return category of tag, if exists
    :rtype: str(enum(tag_categories))
    """
    return _request(_get_category_of_tag, tag_name, auto_open_connection=auto_open_connection)
