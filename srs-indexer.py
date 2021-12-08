import json
import argparse
import pathlib

import config

try:
    from . import common
except ImportError:
    import common
import datetime


MEDIA_TYPE_CODES = {
    0: "image",
    1: "audio",
    2: "video",
    3: "video-loop"
}


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
    add_date = mtime.strftime('%Y-%m-%d %H:%M:%S')
    tag_ids = list()
    origin_name = None
    if "origin" in data['content'] and data['content']['origin'] is not None:
        origin_name = data['content']['origin']
    origin_id = None
    if "id" in data['content'] and data['content']['id'] is not None:
        origin_id = str(data['content']['id'])
    for tag_category in data['content']['tags']:
        for tag in data['content']['tags'][tag_category]:
            tag_name = tag
            tag_alias = tag
            for special_tag_category in ("artist", "set", "original character"):
                if tag_category == special_tag_category:
                    tag_alias = "{}:{}".format(special_tag_category, tag_name)
            sql_select_tag_query = "SELECT ID FROM `tag` WHERE title = %s and category = %s"
            cursor.execute(sql_select_tag_query, (tag_name, tag_category))
            tag_id = cursor.fetchone()
            if tag_id is None:
                sql_insert_tag_query = "INSERT INTO `tag` (`ID`, title, category) VALUES (NULL, %s, %s)"
                cursor.execute(sql_insert_tag_query, (tag_name, tag_category))
                tag_id = cursor.lastrowid
                sql_insert_alias_query = "INSERT INTO `tag_alias` (`ID`, tag_id, title) VALUES (NULL, %s, %s)"
                cursor.execute(sql_insert_alias_query, (tag_id, tag_alias))
            else:
                tag_id = tag_id[0]
            tag_ids.append(tag_id)
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
            add_date,
            origin_name,
            origin_id
        )
    )
    content_id = cursor.lastrowid
    sql_insert_content_id_to_tag_id = "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
    for tag_id in tag_ids:
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
        sql_delete_file_query = "DELETE FROM file_path WHERE file_path = %s"
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

