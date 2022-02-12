import pathlib

from . import files_by_tag_search
from . import common
from . import tags_indexer
from . import srs_indexer


def get_tag_name_by_alias(alias):
    common.open_connection_if_not_opened()
    cursor = common.connection.cursor()
    sql_template = "SELECT title FROM tag WHERE ID = (SELECT tag_id FROM tag_alias WHERE title=%s)"
    cursor.execute(sql_template, (alias,))
    result = cursor.fetchone()[0]
    common.close_connection_if_not_closed()
    return result


def get_file_data_by_file_path(path: pathlib.Path, auto_open_connection=True):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    sql_template = "SELECT * FROM content WHERE file_path=%s"
    cursor = common.connection.cursor()
    cursor.execute(sql_template, (str(path),))
    result = cursor.fetchone()
    if auto_open_connection:
        common.close_connection_if_not_closed()
    return result


def content_update(content_id, content_title, origin_name, origin_id, auto_open_connection):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    sql_template = "UPDATE content SET title=%s, origin=%s, origin_content_id=%s WHERE ID=%s"
    cursor = common.connection.cursor()
    cursor.execute(sql_template, (content_title, origin_name, origin_id, content_id,))
    common.connection.commit()
    if auto_open_connection:
        common.close_connection_if_not_closed()


def get_tags_by_content_id(content_id, auto_open_connection=True):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    sql_template = ("SELECT title, category FROM tag where ID in "
                    "(SELECT tag_id from content_tags_list where content_id=%s)")
    cursor = common.connection.cursor()
    cursor.execute(sql_template, (content_id,))
    result = dict()
    tag = cursor.fetchone()
    while tag is not None:
        if tag[1] not in result:
            result[tag[1]] = [tag[0]]
        else:
            result[tag[1]].append(tag[0])
        tag = cursor.fetchone()
    if auto_open_connection:
        common.close_connection_if_not_closed()
    return result

