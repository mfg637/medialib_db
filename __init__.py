import pathlib

from . import files_by_tag_search
from . import common
from . import tags_indexer
from . import srs_indexer
from . import config


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


def get_thumbnail(path: pathlib.Path, width:int, height:int, _format: str, connection):
    #if auto_open_connection:
    #    common.open_connection_if_not_opened()
    sql_template = "SELECT file_path, format FROM thumbnail WHERE content_id=(SELECT ID FROM content WHERE file_path=%s) and width=%s and height=%s and format=%s"
    cursor = connection.cursor()
    cursor.execute(sql_template, (str(path), width, height, _format))
    result = cursor.fetchone()
    #if auto_open_connection:
    #    common.close_connection_if_not_closed()
    if result is not None:
        return result
    else:
        return None, None


def register_thumbnail(source_file: pathlib.Path, width: int, height: int, _format: str, connection):
    #if auto_open_connection:
    #    common.open_connection_if_not_opened()
    sql_template_get_id = "SELECT ID from content WHERE file_path=%s"
    sql_template_register = "INSERT INTO thumbnail VALUE (%s, %s, %s, NOW(), %s, %s)"
    cursor = connection.cursor()
    print(sql_template_get_id, (str(source_file),))
    cursor.execute(sql_template_get_id, (str(source_file),))
    content_id = cursor.fetchone()
    thumbnail_file_name = None
    if content_id is not None:
        content_id = content_id[0]
        thumbnail_file_name = "{}-{}x{}.{}".format(content_id, width, height, _format.lower())
        cursor.execute(sql_template_register, (content_id, width, height, _format, thumbnail_file_name))
    #if auto_open_connection:
    connection.commit()
    #    common.close_connection_if_not_closed()
    return content_id, thumbnail_file_name


def drop_thumbnails(content_id, connection):
    sql_template_get_id = "DELETE FROM thumbnail WHERE content_id=%s"
    cursor = connection.cursor()
    cursor.execute(sql_template_get_id, (content_id,))
    connection.commit()


def content_update(content_id, content_title, origin_name, origin_id, hidden, description, auto_open_connection):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    sql_template = \
        "UPDATE content SET title=%s, origin=%s, origin_content_id=%s, hidden=%s, description=%s WHERE ID=%s"
    cursor = common.connection.cursor()
    cursor.execute(sql_template, (content_title, origin_name, origin_id, hidden, description, content_id,))
    common.connection.commit()
    if auto_open_connection:
        common.close_connection_if_not_closed()


def content_register(
        content_title,
        file_path,
        content_type,
        addition_date,
        description,
        origin_name,
        origin_id,
        hidden=False,
        *,
        content_id=None,
        auto_open_connection=True
):
    sql_template = "INSERT INTO content VALUE (NULL, %s, %s, %s, %s, %s, %s, %s, %s)"
    if auto_open_connection:
        common.open_connection_if_not_opened()
    cursor = common.connection.cursor()
    cursor.execute(sql_template,
                   (
                       str(file_path),
                       content_title,
                       content_type,
                       description,
                       addition_date,
                       origin_name,
                       origin_id,
                       hidden
                   )
                   )
    content_id = cursor.lastrowid
    common.connection.commit()
    if auto_open_connection:
        common.close_connection_if_not_closed()
    return content_id


def add_tags_for_content(content_id, tags: list[tuple[str, str, str]], auto_open_connection=True):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    for tag in tags:
        tag_id = None
        if tag[1] is not None:
            tag_id = tags_indexer.check_tag_exists(tag[0], tag[1], common.connection)
        else:
            get_id_by_tag_alias_sql = "SELECT tag_id FROM tag_alias WHERE title=%s"
            cursor = common.connection.cursor()
            cursor.execute(get_id_by_tag_alias_sql, (tag[2],))
            tag_id = cursor.fetchone()

        if tag_id is None and tag[1] is not None:
            tag_id = tags_indexer.insert_new_tag(tag[0], tag[1], tag[2], common.connection)
        elif tag_id is not None:
            tag_id = tag_id[0]
        else:
            raise Exception("Not registered tag error", tag[0])


        sql_insert_content_id_to_tag_id = \
            "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
        cursor = common.connection.cursor()
        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))

    if auto_open_connection:
        common.connection.commit()
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


def find_content_from_source(origin, origin_content_id, connection) -> tuple[int, str]:
    """
    Search content by origin content ID
    :rtype: content_id: int, file_path: str
    """
    sql_template = "SELECT ID, file_path FROM content WHERE origin=%s and origin_content_id=%s"
    cursor = connection.cursor()
    cursor.execute(sql_template, (origin, origin_content_id))
    result = cursor.fetchone()
    cursor.fetchall()
    return result


def update_file_path(content_id, file_path, connection):
    sql_template = "UPDATE content SET file_path=%s, addition_date=NOW() WHERE ID=%s"
    cursor = connection.cursor()
    cursor.execute(sql_template, (file_path, content_id))
    connection.commit()
