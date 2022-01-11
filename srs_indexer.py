import json
import argparse
import pathlib

import mysql.connector

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


MEDIA_TYPE_CODES = {
    0: "image",
    1: "audio",
    2: "video",
    3: "video-loop"
}


def register(
        file_path, title, media_type, description, origin, content_id, tags, *, auto_open_connection=True
        ):
    if auto_open_connection:
        common.open_connection_if_not_opened()
    elif common.connection is None:
        raise OSError("connection is closed")
    cursor = common.connection.cursor()

    _tags = list()

    for tag_category in tags:
        _category = tag_category
        if tag_category == "characters":
            _category = "character"
        for tag in tags[tag_category]:
            tag_name = tag
            tag_alias = tag
            for special_tag_category in ("artist", "set", "original character"):
                if tag_category == special_tag_category:
                    tag_alias = "{}:{}".format(special_tag_category, tag_name)
            tag_id = tags_indexer.check_tag_exists(tag_name, _category, auto_open_connection=False)
            if tag_id is None:
                tag_id = tags_indexer.insert_new_tag(tag_name, _category, tag_alias, auto_open_connection=False)
            else:
                tag_id = tag_id[0]
            _tags.append((tag_id, tag_name, _category))
    sql_insert_content_query = (
        "INSERT INTO content "
        "(ID, file_path, title, content_type, description, addition_date, origin, origin_content_id) VALUES"
        "(NULL, %s, %s, %s, %s, NOW(), %s, %s)"
    )
    try:
        cursor.execute(
            sql_insert_content_query,
            (
                str(file_path.relative_to(config.relative_to)),
                title,
                media_type,
                description,
                origin,
                content_id
            )
        )
    except mysql.connector.errors.IntegrityError:
        # triggers in same file path case (file exists)
        return
    content_id = cursor.lastrowid
    sql_insert_content_id_to_tag_id = "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
    for tag in _tags:
        cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag[0]))

    common.connection.commit()
    if auto_open_connection:
        common.close_connection_if_not_closed()


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
    media_type = MEDIA_TYPE_CODES[data['content']['media-type']]
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
            tag_id = tags_indexer.check_tag_exists(tag_name, _category, auto_open_connection=False)
            if tag_id is None:
                tag_id = tags_indexer.insert_new_tag(tag_name, _category, tag_alias, auto_open_connection=False)
            else:
                tag_id = tag_id[0]
            tags.append((tag_id, tag_name, _category))
    sql_insert_content_query = (
        "INSERT INTO content "
        "(ID, file_path, title, content_type, description, addition_date, origin, origin_content_id) VALUES"
        "(NULL, %s, %s, %s, %s, %s, %s, %s)"
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
    content_id = cursor.lastrowid
    sql_insert_content_id_to_tag_id = "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
    for tag in tags:
        try:
            cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag[0]))
        except mysql.connector.IntegrityError:
            get_content_id = "SELECT ID FROM content WHERE file_path=%s"
            get_tag_id = "SELECT ID FROM tag WHERE title=%s and category=%s"
            cursor.execute(get_content_id, (str(file_path.relative_to(config.relative_to)),))
            content_id = cursor.fetchone()
            if content_id is None:
                raise Exception("Database content insertion error")
            else:
                content_id = content_id[0]
            cursor.execute(get_tag_id, tag[1:])
            tag_id = cursor.fetchone()
            if tag_id is None:
                raise Exception("Database tag not exists error", tag[1:])
            else:
                tag_id = tag_id[0]
            cursor.execute(sql_insert_content_id_to_tag_id, (content_id, tag_id))

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
        sql_delete_tags = "DELETE FROM content_tags_list WHERE content_id = (SELECT ID FROM content WHERE file_path = %s)"
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

