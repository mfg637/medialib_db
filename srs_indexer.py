import base64
import json
import argparse
import logging
import pathlib
import dataclasses

import psycopg2.errors

import medialib_db.common

try:
    from . import config
except ImportError:
    import config

try:
    from . import common
except ImportError:
    import common

try:
    from . import tags_indexer
except ImportError:
    import tags_indexer
import datetime

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ContentRepresentationUnit:
    file_path: pathlib.Path
    compatibility_level: int | None
    format: str

    def get_path_str(self):
        return base64.b32encode(str(self.file_path.relative_to(config.relative_to)).encode("utf-8")).decode("utf-8")


MEDIA_TYPE_CODES = {
    0: "image",
    1: "audio",
    2: "video",
    3: "video-loop"
}


def get_content_type(data):
    return MEDIA_TYPE_CODES[data['content']['media-type']]


def srs_parse_representations(file_path: pathlib.Path) -> list[ContentRepresentationUnit]:
    parent_dir = file_path.parent
    results: list[ContentRepresentationUnit] = []
    with file_path.open("r") as srs_file:
        srs_data = json.load(srs_file)
        if get_content_type(srs_data) == "image":
            for level in srs_data["streams"]["image"]["levels"]:
                repr_file_path = parent_dir.joinpath(srs_data["streams"]["image"]["levels"][level])
                format = repr_file_path.suffix[1:].lower()
                results.append(ContentRepresentationUnit(repr_file_path, int(level), format))
    return results


def srs_update_representations(content_id, file_path, cursor):
    sql_remove_representations = (
        "DELETE FROM representations WHERE content_id=%s"
    )
    sql_insert_representation = (
        "INSERT INTO representations (content_id, format, compatibility_level, file_path) "
        "VALUES (%s, %s, %s, %s)"
    )
    cursor.execute(sql_remove_representations, (content_id,))
    representations = srs_parse_representations(file_path)
    for representation in representations:
        cursor.execute(sql_insert_representation, (
            content_id,
            representation.format,
            representation.compatibility_level,
            str(representation.file_path.relative_to(config.relative_to))
        ))


def register(
        file_path: pathlib.Path, title, media_type, description, origin, content_id, tags, connection
        ) -> int:
    """
    Register content in medialib database
    :return: content ID
    """
    cursor = connection.cursor()

    _tags = set()

    sql_check_tag_exists = "SELECT title, category FROM tag WHERE id = %s"

    def verify_tag(tag_id, tag_name=None, tag_category=None):
        cursor.execute(sql_check_tag_exists, (tag_id,))
        tag_verify_data = cursor.fetchone()
        if tag_verify_data is None:
            if tag_name is not None and tag_category is not None:
                raise Exception(
                    "Inserted tag {}({}) actually does\'t exists".format(tag_name, tag_category)
                )
            else:
                raise Exception(
                    "Inserted tag id{} actually does\'t exists".format(tag_id)
                )
        else:
            logger.debug("Tag exists: {}({})".format(tag_verify_data[0], tag_verify_data[1]))

    for tag_category in tags:
        _category = tag_category
        if tag_category == "characters" or tag_category == "original character":
            _category = "character"
        for tag in tags[tag_category]:
            tag_name = tag
            tag_alias = tag
            for special_tag_category in ("artist", "copyright", "character"):
                if tag_category == special_tag_category:
                    tag_alias = "{}:{}".format(special_tag_category, tag_name)
            tag_id = medialib_db.tags_indexer.get_tag_id_by_alias(
                tag_alias, connection
            )
            logger.debug("tag_id={}".format(tag_id.__repr__()))
            if tag_id is None:
                tag_id = tags_indexer.insert_new_tag(tag_name, _category, tag_alias, connection)
                verify_tag(tag_id, tag_name, tag_category)
            else:
                if type(tag_id) is tuple:
                    tag_id = tag_id[0]
            _tags.add((tag_id, tag_name, _category))
    sql_insert_content_query = (
        "INSERT INTO content "
        "(id, file_path, title, content_type, description, addition_date, origin, origin_content_id, hidden) "
        "VALUES (DEFAULT, %s, %s, %s, %s, NOW(), %s, %s, FALSE) RETURNING id"
    )
    try:
        cursor.execute(
            sql_insert_content_query,
            (
                str(file_path.relative_to(config.relative_to)),
                medialib_db.common.postgres_string_format(title, common.CONTENT_TITLE_MAX_SIZE),
                media_type,
                description,
                origin,
                content_id
            )
        )
    except psycopg2.errors.UniqueViolation:
        # triggers in same file path case (file exists)
        return
    content_id = cursor.fetchone()[0]
    sql_insert_content_id_to_tag_id = "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
    for tag in _tags:
        verify_tag(tag[0])
        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag[0]))

    if file_path.suffix == ".srs":
        srs_update_representations(content_id, file_path, cursor)

    connection.commit()
    return content_id


def index(file_path: pathlib.Path, description=None, auto_open_connection=True):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    elif common.connection is None:
        raise OSError("connection is closed")
    cursor = common.connection.cursor()
    sql_check_indexed = "SELECT COUNT(*) FROM content WHERE file_path = %s"
    cursor.execute(sql_check_indexed, (str(file_path.relative_to(config.relative_to)),))
    if cursor.fetchone()[0] > 0:
        print("File exists, skipped")
        if auto_open_connection:
            common.close_connection_if_not_closed()
        return
    f = file_path.open("r")
    data = json.load(f)
    f.close()
    if 'tags' not in data['content']:
        if auto_open_connection:
            common.close_connection_if_not_closed()
        return
    media_type = get_content_type(data)
    content_title = None
    if "title" in data['content'] and data['content']['title'] is not None:
        content_title = data['content']['title']
    mtime = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
    tags = list()
    origin_name = None
    if "origin" in data['content'] and data['content']['origin'] is not None:
        origin_name = data['content']['origin']
    origin_id = None
    if "id" in data['content'] and data['content']['id'] is not None:
        origin_id = str(data['content']['id'])
    for tag_category in data['content']['tags']:
        _category = tag_category
        if tag_category == "characters":
            _category = "character"
        for tag in data['content']['tags'][tag_category]:
            tag_name = tag
            tag_alias = tag
            for special_tag_category in ("artist", "set", "original character"):
                if tag_category == special_tag_category:
                    tag_alias = "{}:{}".format(special_tag_category, tag_name)
            tag_id = tags_indexer.check_tag_exists(tag_name, _category, connection=common.connection)
            if tag_id is None:
                tag_id = tags_indexer.insert_new_tag(
                    tag_name, _category, tag_alias, connection=common.connection
                )
            else:
                tag_id = tag_id[0]
            tags.append((tag_id, tag_name, _category))
    sql_insert_content_query = (
        "INSERT INTO content "
        "(id, file_path, title, content_type, description, addition_date, origin, origin_content_id, hidden)"
        " VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, FALSE) RETURNING id"
    )
    cursor.execute(
        sql_insert_content_query,
        (
            str(file_path.relative_to(config.relative_to)),
            content_title,
            media_type,
            description,
            mtime,
            origin_name,
            origin_id
        )
    )
    content_id = cursor.fetchone()[0]
    sql_insert_content_id_to_tag_id = "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
    for tag in tags:
        # TODO: uncomment try-except block, when same exception from postgresql connector occurs
        #try:
        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag[0]))
        # except mysql.connector.IntegrityError:
        #     get_content_id = "SELECT id FROM content WHERE file_path = %s"
        #     get_tag_id = "SELECT id FROM tag WHERE title = %s and category = %s"
        #     cursor.execute(get_content_id, (str(file_path.relative_to(config.relative_to)),))
        #     content_id = cursor.fetchone()
        #     if content_id is None:
        #         raise Exception("Database content insertion error")
        #     else:
        #         content_id = content_id[0]
        #     cursor.execute(get_tag_id, tag[1:])
        #     tag_id = cursor.fetchone()
        #     if tag_id is None:
        #         raise Exception("Database tag not exists error", tag[1:])
        #     else:
        #         tag_id = tag_id[0]
        #     cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))

    common.connection.commit()
    if auto_open_connection:
        common.close_connection_if_not_closed()


def verify_exists(auto_open_connection=True):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    elif common.connection is None:
        raise OSError("connection is closed")
    print("Verifying file existing…")
    cursor = common.connection.cursor()
    sql_get_file_paths = "SELECT file_path FROM content"
    cursor.execute(sql_get_file_paths)
    deleted_list = list()
    relative_file_path = cursor.fetchone()
    while relative_file_path is not None:
        file_path = config.relative_to.joinpath(relative_file_path[0])
        if not file_path.exists():
            deleted_list.append(relative_file_path[0])
        relative_file_path = cursor.fetchone()

    for file in deleted_list:
        print(file)
        sql_delete_tags = \
            "DELETE FROM content_tags_list WHERE content_id = (SELECT id FROM content WHERE file_path = %s)"
        cursor.execute(sql_delete_tags, (file,))
        sql_delete_file_query = "DELETE FROM content WHERE file_path = %s"
        cursor.execute(sql_delete_file_query, (file,))

    common.connection.commit()
    if auto_open_connection:
        common.close_connection_if_not_closed()


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("path", help="SRS file or directory", type=pathlib.Path)
    args = argparser.parse_args()
    common.open_connection_if_not_opened()
    verify_exists(auto_open_connection=False)
    if pathlib.Path(args.path).is_dir():
        for file in args.path.glob("**/*.srs"):
            print("Processing file ", file)
            index(file, auto_open_connection=False)
    elif pathlib.Path(args.path).is_file():
        index(args.path, auto_open_connection=False)
    common.close_connection_if_not_closed()

