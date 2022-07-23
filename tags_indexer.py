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


def _request(request_body, *args, connection):
    cursor = connection.cursor()
    return request_body(cursor, *args)


def _check_tag_exists(cursor, tag_name:str, tag_category:str):
    sql_select_tag_query = "SELECT ID FROM `tag` WHERE title = %s and category = %s"
    cursor.execute(sql_select_tag_query, (tag_name.replace("_", " "), tag_category))
    # returns ID of tag
    return cursor.fetchone()


def check_tag_exists(tag_name, tag_category, connection) -> tuple[int,]:
    """
    Check exists tag in specified category and returns tag ID.
    :rtype: tuple with ID of tag (tuple(int,))
    """
    return _request(_check_tag_exists, tag_name, tag_category, connection=connection)


def _insert_new_tag(cursor, tag_name: str, tag_category, tag_alias):
    _tag_name = tag_name.replace("_", " ")
    sql_insert_tag_query = "INSERT INTO `tag` (`ID`, title, category) VALUE (NULL, %s, %s)"
    cursor.execute(sql_insert_tag_query, (_tag_name, tag_category))
    tag_id = cursor.lastrowid
    sql_insert_alias_query = "INSERT INTO `tag_alias` (`ID`, tag_id, title) VALUE (NULL, %s, %s)"
    try:
        cursor.execute(sql_insert_alias_query, (tag_id, tag_alias))
    except mysql.connector.errors.IntegrityError as e:
        get_tag_info = "SELECT tag.category, ID from tag where ID=(SELECT tag_id from tag_alias where tag_alias.title=%s)"
        cursor.execute(get_tag_info, (_tag_name,))
        response = cursor.fetchone()
        remove_duplicate = "DELETE FROM tag where ID=%s"
        cursor.execute(remove_duplicate, (tag_id, ))
        _category = response[0]
        tag_id = response[1]
        if _category == "content":
            update_category = "UPDATE tag Set category=%s where ID=%s"
            cursor.execute(update_category, (tag_category, tag_id))
            return tag_id
        elif tag_category == "content":
            return tag_id
        else:
            raise e
    if " " in tag_alias:
        cursor.execute(sql_insert_alias_query, (tag_id, tag_alias.replace(" ", "_")))
    elif "_" in tag_alias:
        cursor.execute(sql_insert_alias_query, (tag_id, tag_alias.replace("_", " ")))
    return tag_id


def insert_new_tag(tag_name, tag_category, tag_alias, connection) -> int:
    """
    Insert new tag in database's table and returns tag ID.
    :rtype: ID of tag (int)
    """
    return _request(_insert_new_tag, tag_name, tag_category, tag_alias, connection=connection)


def _get_category_of_tag(cursor, tag_name):
    def mysql_escafe_quotes(_string):
        return re.sub("\"", "\\\"", _string)

    get_tag_category_query = "SELECT category FROM tag WHERE title=%s;"
    cursor.execute(get_tag_category_query, (tag_name.replace("_", " "),))
    raw_categories = cursor.fetchall()
    if raw_categories is not None:
        categories_list = set()
        for raw_category in raw_categories:
            categories_list.add(raw_category[0])
        if len(categories_list):
            return categories_list
    return None


def get_category_of_tag(tag_name, connection):
    """
    Return category of tag, if exists
    :rtype: str(enum(tag_categories))
    """
    return _request(_get_category_of_tag, tag_name, connection=connection)
