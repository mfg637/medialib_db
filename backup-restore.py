import dataclasses
import pathlib
import datetime
import json

from typing import Any, Iterable

import psycopg2.errors

import common
import config

import xml.dom.minidom

from common.backup import TagUnique, ImageHash, ContentRepresentationElement, AlbumOrder, AlternateSource, \
    ContentDocument, TagDocument, file_template_regex

tag_uniq_ids: dict[TagUnique, int] = dict()
base_path: pathlib.Path = pathlib.Path("medialib-dump")
medialib_base_path = config.relative_to.joinpath("pictures", "medialib")

current_dir = pathlib.Path().absolute()

def write_srs(save_path: pathlib.Path, file_name: str, srs_file_path: pathlib.Path):

    srs_abs_path = current_dir.joinpath(srs_file_path)
    parent_dir_path = srs_abs_path.parent

    raw_data = json.load(srs_abs_path.open("r"))
    video = None
    if 'video' in raw_data['streams']:
        video = raw_data['streams']['video']
    audio_streams = None
    if 'audio' in raw_data['streams']:
        audio_streams = raw_data['streams']['audio']
    subtitle_streams = None
    if 'subtitles' in raw_data['streams']:
        subtitle_streams = raw_data['streams']['subtitles']
    image = None
    if 'image' in raw_data['streams']:
        image = raw_data['streams']['image']
    streams_metadata = (video, image)

    with open(srs_abs_path, "br") as fsrc:
        with open(save_path.joinpath(file_name), "bw") as fd:
            fd.write(fsrc.read())

    files = []
    if audio_streams is not None:
        for audio_stream in audio_streams:
            for channel in audio_stream["channels"]:
                for level in audio_stream["channels"][channel]:
                    files.append(audio_stream["channels"][channel][level])

    for content_type_streams in streams_metadata:
        if content_type_streams is not None:
            for level in content_type_streams["levels"]:
                files.append(content_type_streams["levels"][level])

    for file in files:
        abs_file_path = parent_dir_path.joinpath(file)
        new_file_path = save_path.joinpath(file)
        with open(abs_file_path, "br") as fsrc:
            with open(new_file_path, "bw") as fd:
                fd.write(fsrc.read())


def write_mpd(save_path: pathlib.Path, file_name: str, mpd_file_path: pathlib.Path):

    list_files = []

    mpd_file = mpd_abs_path = current_dir.joinpath(mpd_file_path)

    file_templates = set()
    parent_dir = mpd_file.parent
    mpd_document: xml.dom.minidom.Document = xml.dom.minidom.parse(str(mpd_file))
    segment_templates: Iterable[xml.dom.minidom.Element] = mpd_document.getElementsByTagName("SegmentTemplate")
    for template in segment_templates:
        file_templates.add(file_template_regex.sub("*", template.getAttribute("initialization")))
        file_templates.add(file_template_regex.sub("*", template.getAttribute("media")))
    #logger.debug(file_templates.__repr__())

    file_templates_iterable: tuple[str] = tuple(file_templates)
    for file_template in file_templates_iterable:
        for file in parent_dir.glob(file_template):
            if file.is_file():
                list_files.append(file)

    mpd_new_file_path = save_path.joinpath(file_name)
    with open(mpd_file, "br") as fsrc:
        with open(mpd_new_file_path, "bw") as fd:
            fd.write(fsrc.read())

    for file in list_files:
        new_file_path = save_path.joinpath(file.name)
        with open(file, "br") as fsrc:
            with open(new_file_path, "bw") as fd:
                fd.write(fsrc.read())


def write_regular(output_file_path: pathlib.Path, src_file_path: pathlib.Path):
    global checksums

    abs_path = current_dir.joinpath(src_file_path)

    with open(abs_path, "br") as fsrc:
        with open(output_file_path, "bw") as fd:
            fd.write(fsrc.read())

def load_file(file, hash_list):
    pass

registered_tags: set[TagUnique] = set()

sql_register_tag = (
    "INSERT INTO tag (title, category, parent) VALUES (%s, %s, %s) RETURNING id"
)
sql_register_tag_alias = (
    "INSERT INTO tag_alias (tag_id, title) VALUES (%s, %s)"
)
sql_get_tag_id = (
    "SELECT id FROM tag WHERE title = %s and category = %s LIMIT 1"
)

def register_tag(tag_label: TagUnique, cursor) -> int:
    if tag_label in registered_tags:
        cursor.execute(sql_get_tag_id, (tag_label.title, tag_label.category))
        return cursor.fetchone()[0]
    serialised_tag_id = tag_uniq_ids[tag_label]
    with open(base_path.joinpath("tags", f"{serialised_tag_id}.json"), "r") as f:
        raw_tag_data = json.load(f)
    parent_tag = None
    if raw_tag_data["parent"] is not None:
        parent_tag = TagUnique(raw_tag_data["parent"]["title"], raw_tag_data["parent"]["category"])
    tag_data = TagDocument(
        raw_tag_data["title"], raw_tag_data["category"], raw_tag_data["aliases"], parent_tag
    )

    parent_id = None
    if parent_tag is not None:
        parent_id = register_tag(parent_tag, cursor)

    cursor.execute(sql_register_tag, (tag_data.title, tag_data.category, parent_id))
    tag_id = cursor.fetchone()

    for tag_alias in tag_data.aliases:
        cursor.execute(sql_register_tag_alias, (tag_id, tag_alias))

    registered_tags.add(tag_label)
    return tag_id

sql_insert_content = (
    "INSERT INTO content (file_path, title, content_type, description, addition_date, origin, origin_content_id, hidden) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    "RETURNING id"
)
sql_bind_content_tags = (
    "INSERT INTO content_tags_list (content_id, tag_id) VALUES (%s, %s)"
)
sql_register_representations = (
    "INSERT INTO representations (content_id, format, compatibility_level, file_path) "
    "VALUES (%s, %s, %s, %s)"
)
sql_register_imagehash = (
    "INSERT INTO imagehash (content_id, aspect_ratio, hue_hash, saturation_hash, value_hash) "
    "VALUES (%s, %s,%s, %s, %s)"
)
sql_register_album = (
    "INSERT INTO album (set_tag_id, album_artist_tag_id) VALUES (%s, %s) RETURNING id"
)
sql_bind_content_to_album = (
    "INSERT INTO album_order (album_id, content_id, \"order\") "
    "VALUES (%s, %s, %s)"
)
sql_get_album_id = (
    "SELECT id FROM album WHERE set_tag_id = %s and album_artist_tag_id = %s"
)
sql_register_alternate_source = (
    "INSERT INTO alternate_sources (content_id, origin, origin_content_id) "
    "VALUES (%s, %s, %s)"
)
sql_get_content_by_origin = (
    "SELECT id FROM content WHERE origin = %s and origin_content_id = %s"
)

def register_album(artist_tag: TagUnique, set_tag: TagUnique, cursor):
    cursor.execute(sql_get_tag_id, (artist_tag.title, artist_tag.category))
    artist_tag_id = cursor.fetchone()[0]
    cursor.execute(sql_get_tag_id, (set_tag.title, set_tag.category))
    set_tag_id = cursor.fetchone()[0]
    cursor.execute(sql_get_album_id, (set_tag_id, artist_tag_id))
    album_id = cursor.fetchone()
    if album_id is None:
        cursor.execute(sql_register_album, (set_tag_id, artist_tag_id))
        return cursor.fetchone()[0]
    else:
        return album_id[0]

def register_content(serialised_content_file: pathlib.Path, connection):
    with open(serialised_content_file, "r") as f:
        raw_content_data = json.load(f)
    tags: set[TagUnique] = set()
    for raw_tag in raw_content_data["tags"]:
        tag = TagUnique(raw_tag["title"], raw_tag["category"])
        tags.add(tag)
    date = datetime.datetime.fromisoformat(raw_content_data["addition_date"])
    file_suffix = pathlib.PurePath(raw_content_data["file_path"]).suffix
    src_content_id = raw_content_data["content_id"]
    if file_suffix in {".srs", ".mpd"}:
        src_file_path = current_dir.joinpath(
            "medialib-dump", "content", str(src_content_id), f"{src_content_id}{file_suffix}"
        )
    else:
        src_file_path = current_dir.joinpath(
            "medialib-dump", "content", f"{src_content_id}{file_suffix}"
        )
    save_path = medialib_base_path.joinpath(str(date.year), str(date.month), str(date.day))
    save_path.mkdir(parents=True, exist_ok=True)
    file_name = pathlib.PurePath(raw_content_data["file_path"]).name
    file_path = save_path.joinpath(file_name)
    representations_list: list[ContentRepresentationElement] | None = None
    if raw_content_data["representations"] is not None:
        representations_list = []
        for raw_representation in raw_content_data["representations"]:
            repr_file_name = pathlib.PurePath(raw_representation["file_path"]).name
            repr_file_path = save_path.joinpath(repr_file_name)
            representation_unit = ContentRepresentationElement(
                raw_representation["format"],
                raw_representation["compatibility_level"],
                repr_file_path
            )
            representations_list.append(representation_unit)
    image_hash: ImageHash | None = None
    if raw_content_data["imagehash"] is not None:
        image_hash = ImageHash(
            raw_content_data["imagehash"]["aspect_ratio"],
            raw_content_data["imagehash"]["value_hash"],
            raw_content_data["imagehash"]["hue_hash"],
            raw_content_data["imagehash"]["saturation_hash"],
            raw_content_data["imagehash"]["alternate_version"]
        )
    alternate_sources: list[AlternateSource] = []
    for alt_src_raw in raw_content_data["alternate_sources"]:
        alt_src = AlternateSource(alt_src_raw["origin_name"], alt_src_raw["origin_content_id"])
        alternate_sources.append(alt_src)
    albums: list[AlbumOrder] | None = None
    if raw_content_data["albums"] is not None:
        albums = []
        for album_order_raw in raw_content_data["albums"]:
            set_tag: TagUnique = TagUnique(
                album_order_raw["set_tag"]["title"],
                album_order_raw["set_tag"]["category"]
            )
            artist_tag: TagUnique = TagUnique(
                album_order_raw["artist_tag"]["title"],
                album_order_raw["artist_tag"]["category"]
            )
            album_order = AlbumOrder(set_tag, artist_tag, album_order_raw["order"])
            albums.append(album_order)
    content_document = ContentDocument(
        raw_content_data["content_id"],
        file_path,
        raw_content_data["title"],
        raw_content_data["content_type"],
        raw_content_data["description"],
        date,
        raw_content_data["origin"],
        raw_content_data["origin_content_id"],
        raw_content_data["is_hidden"],
        tags,
        alternate_sources,
        image_hash,
        representations_list,
        albums
    )
    cursor = connection.cursor()
    try:
        cursor.execute(sql_insert_content, (
            str(content_document.file_path),
            content_document.title,
            content_document.content_type,
            content_document.description,
            content_document.addition_date,
            content_document.origin,
            content_document.origin_content_id,
            content_document.is_hidden
        ))
    except psycopg2.errors.UniqueViolation:
        cursor.close()
        connection.rollback()
        cursor = connection.cursor()
        cursor.execute(sql_get_content_by_origin, (content_document.origin, content_document.origin_content_id))
        content_id = cursor.fetchone()[0]
        print("founded duplicate ", content_document.origin, content_document.origin_content_id)
        cursor.close()
        return content_id
    content_id = cursor.fetchone()[0]
    for tag in content_document.tags:
        cursor.execute(sql_get_tag_id, (tag.title, tag.category))
        tag_id = cursor.fetchone()[0]
        cursor.execute(sql_bind_content_tags, (content_id, tag_id))
    if content_document.albums is not None:
        for album in content_document.albums:
            album_id = register_album(album.artist_tag, album.set_tag, cursor)
            cursor.execute(sql_bind_content_to_album, (album_id, content_id, album.order))
    if content_document.representations is not None:
        for representation_unit in content_document.representations:
            cursor.execute(sql_register_representations, (
                content_id,
                representation_unit.format,
                representation_unit.compatibility_level,
                str(representation_unit.file_path)
            ))
    if content_document.imagehash is not None:
        cursor.execute(sql_register_imagehash, (
            content_id,
            content_document.imagehash.aspect_ratio,
            content_document.imagehash.hue_hash,
            content_document.imagehash.saturation_hash,
            content_document.imagehash.value_hash
        ))
    if content_document.alternate_sources is not None:
        for alt_src in content_document.alternate_sources:
            cursor.execute(sql_register_alternate_source, (content_id, alt_src.origin_name, alt_src.origin_content_id))
    cursor.close()
    connection.commit()

    try:
        if content_document.file_path.suffix == ".srs":
            write_srs(save_path, file_name, src_file_path)
        elif content_document.file_path.suffix == ".mpd":
            write_mpd(save_path, file_name, src_file_path)
        else:
            write_regular(file_path, src_file_path)
    except FileNotFoundError as e:
        print("File not found \"{}\", content id = {}!".format(e.filename, content_id))
    except xml.parsers.expat.ExpatError as e:
        print("Invalid MPD file, content id = {}!".format(content_id))

    return content_id



def main():
    with open(base_path.joinpath("tag_uniq_id.json"), "r") as f:
        tag_uniq_ids_raw = json.load(f)

    for item in tag_uniq_ids_raw:
        tag_id: int = item[2]
        tag_uniq_ids[TagUnique(item[0], item[1])] = tag_id

    connection = common.make_connection()

    print("register tags")

    for tag_label in tag_uniq_ids:
        if tag_label not in registered_tags:
            cursor = connection.cursor()

            register_tag(tag_label, cursor)

            cursor.close()
            connection.commit()

    print("register content")

    serialises_content_data_path = base_path.joinpath("content-metadata")
    for content_file in serialises_content_data_path.iterdir():
        if content_file.name[0] != '.':
            register_content(content_file, connection)
            connection.commit()

    connection.close()


if __name__ == "__main__":
    main()
