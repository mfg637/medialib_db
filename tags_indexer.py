import logging
import psycopg2.errors
import re

import psycopg2

try:
    from . import config
except ImportError:
    import config

try:
    from . import common
except ImportError:
    import common

logger = logging.getLogger(__name__)


def _request(request_body, *args, connection):
    #print(connection, args)
    cursor = connection.cursor()
    return request_body(cursor, *args)


def _check_tag_exists(cursor, tag_name: str, tag_category: str):
    sql_select_tag_query = "SELECT ID FROM tag WHERE title=%s and category=%s"
    _tag_name = tag_name.replace("_", " ")
    logger.debug("query=\"{}\" title=\"{}\" category=\"{}\"".format(sql_select_tag_query, _tag_name, tag_category))
    cursor.execute(sql_select_tag_query, (_tag_name, tag_category))
    id = cursor.fetchone()
    if id is None:
        sql_get_id_of_alias = \
            "SELECT ID FROM tag WHERE id = (SELECT tag_id FROM tag_alias WHERE tag_alias.title = %s) and category=%s"
        cursor.execute(sql_get_id_of_alias, (_tag_name, tag_category))
        id = cursor.fetchone()
    return id


def check_tag_exists(tag_name, tag_category, connection) -> tuple[int,]:
    """
    Check exists tag in specified category and returns tag ID.
    :rtype: tuple with ID of tag (tuple(int,))
    """
    return _request(_check_tag_exists, tag_name, tag_category, connection=connection)


def _insert_new_tag(connection, tag_name: str, tag_category, tag_alias=None):
    cursor = connection.cursor()
    if tag_alias is None:
        tag_alias = tag_name
        if tag_category == "character":
            tag_alias = "character:{}".format(tag_name)
        elif tag_category == "artist":
            tag_alias = "artist:{}".format(tag_name)
    _tag_name = tag_name.replace("_", " ")
    sql_insert_tag_query = "INSERT INTO tag (id, title, category) VALUES (DEFAULT, %s, %s) RETURNING id"
    try:
        cursor.execute(sql_insert_tag_query, (_tag_name, tag_category))
    except psycopg2.errors.UniqueViolation:
        connection.rollback()
        return _check_tag_exists(cursor, _tag_name, tag_category)
    tag_id = cursor.fetchone()[0]
    logger.debug("_insert_new_tag last row id={}".format(tag_id))
    sql_insert_alias_query = "INSERT INTO tag_alias (tag_id, title) VALUES (%s, %s)"
    logger.debug("tag {} ({}) alias insert: {} ".format(
        _tag_name,
        tag_category,
        tag_alias
    ))
    try:
        cursor.execute(sql_insert_alias_query, (tag_id, tag_alias))
    except psycopg2.IntegrityError as e:
        cursor.connection.rollback()
        get_tag_info = (
            "SELECT tag.category, ID from tag "
            "where id=(SELECT tag_id from tag_alias where tag_alias.title=%s)"
        )
        cursor.execute(get_tag_info, (_tag_name,))
        response = cursor.fetchone()
        remove_duplicate = "DELETE FROM tag where id = %s"
        cursor.execute(remove_duplicate, (tag_id, ))
        _category = response[0]
        tag_id = response[1]
        if _category == "content":
            update_category = "UPDATE tag set category = %s where id = %s"
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
    connection.commit()
    return tag_id


def insert_new_tag(tag_name, tag_category, tag_alias, connection) -> int:
    """
    Insert new tag in database's table and returns tag ID.
    :rtype: ID of tag (int)
    """
    return _insert_new_tag(connection, tag_name, tag_category, tag_alias)


def _get_category_of_tag(cursor, tag_name):
    def mysql_escafe_quotes(_string):
        return re.sub("\"", "\\\"", _string)

    get_tag_category_query = "SELECT category FROM tag WHERE title = %s;"
    cursor.execute(get_tag_category_query, (tag_name.replace("_", " "),))
    raw_categories = cursor.fetchall()
    if raw_categories is None:
        get_tag_category_by_alias = \
            "SELECT category FROM tag where id = (SELECT tag_id FROM tag_alias where title = %s)"
        cursor.execute(get_tag_category_by_alias, (tag_name.replace("_", " "),))
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

def wildcard_tag_search(wildcard_string:str, connection):
    sql_tag_alias_search = (
        "SELECT * FROM tag_alias WHERE title like %s"
    )
    cursor = connection.cursor()
    sql_wildcard_string = wildcard_string.replace("*", "%")
    cursor.execute(sql_tag_alias_search, (sql_wildcard_string,))
    result = cursor.fetchall()
    cursor.close()
    return result

def get_tag_info_by_tag_id(tag_id, connection):
    sql_get_tag_id = "SELECT * FROM tag where ID=%s"
    cursor = connection.cursor()
    cursor.execute(sql_get_tag_id, (tag_id,))
    result = cursor.fetchone()
    cursor.close()
    return result
