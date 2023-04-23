import dataclasses
import pathlib

from . import files_by_tag_search
from . import common
from . import testing
from . import tags_indexer
from . import srs_indexer
from . import config
from . import openclip_classification


def get_tag_name_by_alias(alias):
    connection = common.make_connection()
    cursor = connection.cursor()
    sql_template = "SELECT title FROM tag WHERE id = (SELECT tag_id FROM tag_alias WHERE title=%s)"
    cursor.execute(sql_template, (alias,))
    result = cursor.fetchone()[0]
    connection.close()
    return result


def get_tag_name_by_id(id):
    connection = common.make_connection()
    cursor = connection.cursor()
    sql_template = "SELECT title FROM tag WHERE id = %s"
    cursor.execute(sql_template, (id,))
    result = cursor.fetchone()[0]
    connection.close()
    return result


def get_content_metadata_by_file_path(path: pathlib.Path, connection):
    cursor = connection.cursor()
    sql_template = "SELECT * FROM content WHERE file_path=%s"
    cursor.execute(sql_template, (str(path),))
    result = cursor.fetchone()
    return result


def get_content_metadata_by_content_id(content_id: int, connection):
    cursor = connection.cursor()
    sql_template = "SELECT * FROM content WHERE id=%s"
    cursor.execute(sql_template, (content_id,))
    result = cursor.fetchone()
    return result


def get_thumbnail_by_filepath(path: pathlib.Path, width:int, height:int, _format: str, connection):
    sql_template = ("SELECT file_path, format FROM thumbnail "
                    "WHERE content_id = (SELECT ID FROM content WHERE file_path=%s) "
                    "and width = %s and height = %s and format = %s")
    cursor = connection.cursor()
    cursor.execute(sql_template, (str(path), width, height, _format))
    result = cursor.fetchone()
    if result is not None:
        return result
    else:
        return None, None


def get_thumbnail_by_content_id(content_id: int, width:int, height:int, _format: str, connection):
    sql_template = ("SELECT file_path, format FROM thumbnail "
                    "WHERE content_id = %s "
                    "and width = %s and height = %s and format = %s")
    cursor = connection.cursor()
    cursor.execute(sql_template, (content_id, width, height, _format))
    result = cursor.fetchone()
    if result is not None:
        return result
    else:
        return None, None


def register_thumbnail_by_file_path(source_file: pathlib.Path, width: int, height: int, _format: str, connection):
    sql_template_get_id = "SELECT id from content WHERE file_path=%s"
    cursor = connection.cursor()
    cursor.execute(sql_template_get_id, (str(source_file),))
    content_id = cursor.fetchone()
    if content_id is not None:
        content_id = content_id[0]
    cursor.close()
    return content_id, register_thumbnail_by_content_id(content_id, width, height, _format, connection)


def register_thumbnail_by_content_id(content_id: int, width: int, height: int, _format: str, connection):
    sql_template_register = "INSERT INTO thumbnail VALUES (%s, %s, %s, NOW(), %s, %s)"
    cursor = connection.cursor()
    thumbnail_file_name = "{}-{}x{}.{}".format(content_id, width, height, _format.lower())
    cursor.execute(sql_template_register, (content_id, width, height, _format, thumbnail_file_name))
    connection.commit()
    return thumbnail_file_name


def drop_thumbnails(content_id, connection):
    sql_template_get_id = "DELETE FROM thumbnail WHERE content_id = %s"
    cursor = connection.cursor()
    cursor.execute(sql_template_get_id, (content_id,))
    connection.commit()


def content_update(content_id, content_title, origin_name, origin_id, hidden, description, connection):
    cursor = connection.cursor()
    sql_template = (
        "UPDATE content "
        "SET title = %s, origin = %s, origin_content_id = %s, hidden = %s, description = %s "
        "WHERE id = %s"
    )
    cursor.execute(sql_template, (content_title, origin_name, origin_id, hidden, description, content_id,))
    connection.commit()


def content_register(
        connection,
        content_title,
        file_path,
        content_type,
        addition_date,
        description,
        origin_name,
        origin_id,
        hidden=False,
        *,
        content_id=None
):
    sql_template = "INSERT INTO content VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
    cursor = connection.cursor()
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
    content_id = cursor.fetchone()[0]
    connection.commit()
    return content_id


sql_insert_content_id_to_tag_id = \
    "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"


def add_tags_for_content(content_id, tags: list[tuple[str, str, str]], connection):
    cursor = connection.cursor()
    for tag in tags:
        tag_id = None
        if tag[1] is not None:
            tag_id = tags_indexer.check_tag_exists(tag[0], tag[1], connection)
        else:
            get_id_by_tag_alias_sql = "SELECT tag_id FROM tag_alias WHERE title = %s"
            cursor.execute(get_id_by_tag_alias_sql, (tag[2],))
            tag_id = cursor.fetchone()

        if tag_id is None and tag[1] is not None:
            tag_id = tags_indexer.insert_new_tag(tag[0], tag[1], tag[2], connection)
        elif tag_id is not None:
            tag_id = tag_id[0]
        else:
            raise Exception("Not registered tag error", tag[0])

        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))

    connection.commit()


def add_tags_for_content_by_tag_ids(content_id, tag_ids: list[int], connection):
    cursor = connection.cursor()
    for tag_id in tag_ids:
        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))

    connection.commit()


def connect_tag_by_id(content_id, tag_id, connection):
    cursor = connection.cursor()
    sql_validate_tag_connected = "SELECT * FROM content_tags_list WHERE content_id = %s and tag_id = %s"
    cursor.execute(sql_validate_tag_connected, (content_id, tag_id))
    connection_exists = cursor.fetchone()
    if connection_exists is None:
        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))
    connection.commit()


def get_tags_by_content_id(content_id, auto_open_connection=True):
    connection = common.make_connection()
    cursor = connection.cursor()
    sql_template = ("SELECT id, title, category FROM tag where id in "
                    "(SELECT tag_id from content_tags_list where content_id = %s)")
    cursor.execute(sql_template, (content_id,))
    result = dict()
    tag = cursor.fetchone()
    while tag is not None:
        if tag[2] not in result:
            result[tag[2]] = [(tag[0], tag[1])]
        else:
            result[tag[2]].append((tag[0], tag[1]))
        tag = cursor.fetchone()
    connection.close()
    return result


def find_content_from_source(origin, origin_content_id, connection) -> tuple[int, str]:
    """
    Search content by origin content ID
    :rtype: content_id: int, file_path: str
    """
    sql_template = "SELECT ID, file_path FROM content WHERE origin = %s and origin_content_id = %s"
    cursor = connection.cursor()
    cursor.execute(sql_template, (origin, origin_content_id))
    result = cursor.fetchone()
    cursor.fetchall()
    return result


def update_file_path(content_id, file_path: pathlib.Path, image_hash, connection):
    sql_template = "UPDATE content SET file_path = %s, addition_date=NOW() WHERE ID = %s"
    cursor = connection.cursor()
    cursor.execute(sql_template, (str(file_path.relative_to(config.relative_to)), content_id))
    if file_path.suffix == ".srs":
        srs_indexer.srs_update_representations(content_id, file_path, cursor)
    connection.commit()
    if image_hash is not None:
        set_image_hash(content_id, image_hash, connection)


def get_representation_by_content_id(content_id, connection) -> list[srs_indexer.ContentRepresentationUnit]:
    sql_get_representations = (
        "SELECT format, compatibility_level, file_path FROM representations WHERE content_id=%s"
        " ORDER BY compatibility_level"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_representations, (content_id,))
    results = []
    raw_representation = cursor.fetchone()
    while raw_representation is not None:
        results.append(srs_indexer.ContentRepresentationUnit(
            config.relative_to.joinpath(raw_representation[2]),
            raw_representation[1],
            raw_representation[0]
        ))
        raw_representation = cursor.fetchone()
    cursor.close()
    return results


def set_image_hash(content_id: int, image_hash: tuple[float, bytes, int, int], connection):
    """
    Write hash of image content to database.
    :param image_hash: is a tuple of:
        aspect ratio — 32bit float;
        value_hash — 64bit integer;
        hs_hash — 16bit hue and saturation hashes joint to 32bit integer;
    """
    sql_verify_hash_exists = "SELECT * FROM imagehash WHERE content_id=%s"
    sql_insert_image_hash = (
        "INSERT INTO imagehash (content_id, aspect_ratio, value_hash, hue_hash, saturation_hash) "
        "VALUES (%s, %s, decode(%s, 'hex'), %s, %s)"
    )
    sql_update_image_hash = (
        "UPDATE imagehash SET aspect_ratio = %s, value_hash = decode(%s, 'hex'), hue_hash = %s, saturation_hash = %s "
        "WHERE content_id = %s"
    )
    aspect_ratio, value_hash, hue_hash, saturation_hash = image_hash
    cursor = connection.cursor()
    cursor.execute(sql_verify_hash_exists, (content_id,))
    exists_hash_data = cursor.fetchone()
    if exists_hash_data is None:
        cursor.execute(sql_insert_image_hash, (content_id, aspect_ratio, value_hash.hex(), hue_hash, saturation_hash))
    else:
        cursor.execute(sql_update_image_hash, (aspect_ratio, value_hash.hex(), hue_hash, saturation_hash, content_id))
    connection.commit()
    cursor.close()


def get_image_hash(content_id, connection):
    sql_get_hash = "SELECT * FROM imagehash WHERE content_id=%s"
    cursor = connection.cursor()
    cursor.execute(sql_get_hash, (content_id,))
    exists_hash_data = cursor.fetchone()
    cursor.close()
    return exists_hash_data


def get_album_title(album_id, connection):
    sql_get_album_title = (
        "select (select title from tag where tag.id = album.set_tag_id), "
        "(select title from tag where tag.id = album.album_artist_tag_id) "
        "from album where ID = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_album_title, (album_id,))
    result = cursor.fetchone()
    cursor.close()
    return result


def get_album_content(album_id, connection):
    sql_get_content_by_album_id = (
        "select content.ID, file_path, content_type, title, description, origin, origin_content_id, \"order\" "
        "from album_order join content on album_order.content_id = content.ID "
        "where album_id = %s order by album_order.\"order\";"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_content_by_album_id, (album_id,))
    result = cursor.fetchall()
    cursor.close()
    return result


def get_album_related_content(set_tag_id, artist_tag_id, connection):

    sql_get_content_by_album_id = (
        "select content.id, file_path, content_type, title, description, origin, origin_content_id, album_order.\"order\" "
        "from content left outer join album_order "
        "on content.id = album_order.content_id "
        "where content.id in "
        "(SELECT content_id FROM content_tags_list WHERE tag_id = %s) "
        "and content.id in "
        "(SELECT content_id FROM content_tags_list WHERE tag_id = %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_content_by_album_id, (set_tag_id, artist_tag_id))
    result = cursor.fetchall()
    cursor.close()
    return result

def get_album_id(set_tag_id, artist_tag_id, connection):
    sql_get_album_id = "select id from album where set_tag_id = %s and album_artist_tag_id = %s"
    cursor = connection.cursor()
    cursor.execute(sql_get_album_id, (set_tag_id, artist_tag_id))
    result = cursor.fetchone()
    if result is not None:
        result = result[0]
    cursor.close()
    return result

def make_album(set_tag_id, artist_tag_id, connection):
    sql_register_album = (
        "INSERT INTO album VALUES (DEFAULT, %s, %s) RETURNING id"
    )
    cursor = connection.cursor()
    cursor.execute(sql_register_album, (set_tag_id, artist_tag_id))
    result = cursor.fetchone()[0]
    cursor.close()
    return result

def set_album_order(album_id, content_id, order, connection):
    sql_verify_content_registered = (
        "select * from album_order where album_id = %s and content_id = %s"
    )
    sql_insert_content_order = (
        "INSERT INTO album_order VALUES (%s, %s, %s)"
    )
    sql_update_order = (
        "UPDATE album_order SET \"order\" = %s WHERE album_id = %s and content_id = %s"
    )
    sql_delete_content_from_album = (
        "DELETE FROM album_order WHERE album_id = %s and content_id = %s"
    )
    cursor = connection.cursor()
    cursor.execute(sql_verify_content_registered, (album_id, content_id))
    content_info = cursor.fetchone()
    if content_info is None:
        if order is not None:
            cursor.execute(sql_insert_content_order, (album_id, content_id, order))
    else:
        if order is not None:
            if content_info[2] != order:
                cursor.execute(sql_update_order, (order, album_id, content_id))
        else:
            cursor.execute(sql_delete_content_from_album, (album_id, content_id))

def get_content_albums(content_id, connection):
    sql_get_albums_by_content_id = (
        "select ID, (select title from tag where tag.id = album.set_tag_id), "
        "(select title from tag where tag.id = album.album_artist_tag_id) "
        "from album where id in "
        "(select album_id from album_order where content_id = %s)"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_albums_by_content_id, (content_id,))
    results = cursor.fetchall()
    cursor.close()
    return results

def get_album_covers(connection):
    sql_get_album_covers = (
        "select content.id, file_path, content_type, "
        "CONCAT((select title from tag where tag.id = album.set_tag_id), ' by ', "
        "(select title from tag where tag.id = album.album_artist_tag_id)), "
        "album.id "
        "from album, lateral "
        "(select * from album_order where album.id = album_order.album_id order by album_order.\"order\" limit 1) ao "
        "join content on content.id = ao.content_id"
    )
    cursor = connection.cursor()
    cursor.execute(sql_get_album_covers, tuple())
    results = cursor.fetchall()
    cursor.close()
    return results


@dataclasses.dataclass
class DuplicatedContentItem:
    content_id: int
    file_path: str
    content_type: str
    title: str
    is_alternate_version: bool
    content_metadata = None


@dataclasses.dataclass
class DuplicateImageHashItem:
    value_hash: bytes
    hue_hash: int
    saturation_hash: int
    duplicated_images: list[DuplicatedContentItem]


def find_duplicates(connection, show_alternates=False):
    sql_find_duplicates_and_hide_alternates = (
        "select encode(value_hash, 'hex'), hue_hash, saturation_hash from "
        "(select count(*) as c1, value_hash, hue_hash, saturation_hash, alternate_version from imagehash "
        "group by value_hash, hue_hash, saturation_hash, alternate_version) as u1 "
        "where u1.c1 > 1 and u1.alternate_version = false;"
    )
    sql_find_duplicates_and_alternates = (
        "select encode(value_hash, 'hex'), hue_hash, saturation_hash from "
        "(select count(*) as c1, value_hash, hue_hash, saturation_hash from imagehash "
        "group by value_hash, hue_hash, saturation_hash) as u1 "
        "where u1.c1 > 1;"
    )
    sql_find_duplicates_by_hash = (
        "select content.ID, file_path, content_type, title, alternate_version "
        "from content join imagehash on content.ID = imagehash.content_id "
        "where value_hash = decode(%s, 'hex') and hue_hash = %s and saturation_hash = %s;"
    )
    results: list[DuplicateImageHashItem] = []
    cursor = connection.cursor()
    if show_alternates:
        cursor.execute(sql_find_duplicates_and_alternates, tuple())
    else:
        cursor.execute(sql_find_duplicates_and_hide_alternates, tuple())
    image_hash_list = cursor.fetchall()
    for image_hash in image_hash_list:
        hash_item = DuplicateImageHashItem(image_hash[0], image_hash[1], image_hash[2], [])
        cursor.execute(sql_find_duplicates_by_hash, (
            hash_item.value_hash, hash_item.hue_hash, hash_item.saturation_hash
        ))
        image_data = cursor.fetchone()
        while image_data is not None:
            image_item = DuplicatedContentItem(
                *image_data
            )
            hash_item.duplicated_images.append(image_item)
            image_data = cursor.fetchone()
        results.append(hash_item)
    cursor.close()
    return results

