import dataclasses
import io
import pathlib
import json
import tarfile
import xml.dom
import xml.dom.minidom
from typing import Iterable

import crcmod

import common
import common.backup
import config
from common.backup import TagUnique, ImageHash, ContentRepresentationElement, AlbumOrder, AlternateSource, \
    ContentDocument, TagDocument, file_template_regex

tags_uniq: set[TagUnique] = set()


def write_srs(tar_dump, srs_file_path: pathlib.Path, content_id):
    global checksums

    srs_abs_path = config.relative_to.joinpath(srs_file_path)
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

    file_hash = 0
    with srs_abs_path.open("br") as f:
        file_hash = crc64(f.read())
    srs_new_file_path = pathlib.PurePath("content/{}/{}.srs".format(content_id, content_id))
    checksums.append((file_hash, srs_new_file_path))
    tar_dump.add(name=str(srs_abs_path), arcname=str(srs_new_file_path))

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
        file_hash = 0
        abs_file_path = parent_dir_path.joinpath(file)
        with abs_file_path.open("br") as f:
            file_hash = crc64(f.read())
        new_file_path = pathlib.PurePath("content/{}/{}".format(content_id, file))
        checksums.append((file_hash, new_file_path))
        tar_dump.add(name=str(abs_file_path), arcname=str(new_file_path))


def write_mpd(tar_dump, mpd_file_path, content_id):
    global checksums

    list_files = []

    mpd_file = mpd_abs_path = config.relative_to.joinpath(mpd_file_path)

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

    file_hash = 0
    with mpd_abs_path.open("br") as f:
        file_hash = crc64(f.read())
    mpd_new_file_path = pathlib.PurePath("content/{}/{}.mpd".format(content_id, content_id))
    checksums.append((file_hash, mpd_new_file_path))
    tar_dump.add(name=str(mpd_abs_path), arcname=str(mpd_new_file_path))

    for file in list_files:
        file_hash = 0
        with file.open("br") as f:
            file_hash = crc64(f.read())
        new_file_path = pathlib.PurePath("content/{}/{}".format(content_id, file.name))
        checksums.append((file_hash, new_file_path))
        tar_dump.add(name=str(file), arcname=str(new_file_path))


def write_regular(tar_dump, file_path: pathlib.Path, content_id):
    global checksums

    abs_path = config.relative_to.joinpath(file_path)

    file_hash = 0
    with abs_path.open("br") as f:
        file_hash = crc64(f.read())
    srs_new_file_path = pathlib.PurePath("content/{}{}".format(content_id, file_path.suffix))
    checksums.append((file_hash, srs_new_file_path))
    tar_dump.add(name=str(abs_path), arcname=str(srs_new_file_path))


crc64 = crcmod.predefined.mkCrcFun("crc-64")
checksums: list[tuple[int, pathlib.PurePath]] = []


def create_tar_file(tar_dump: tarfile.TarFile, buffer: io.StringIO, path: pathlib.PurePath, write_crc=True):
    encoded_data = buffer.getvalue().encode("utf-8")
    if write_crc:
        data_crc64 = crc64(encoded_data)
        checksums.append((data_crc64, path))
    data_buffer = io.BytesIO(encoded_data)
    tar_tag_info = tarfile.TarInfo(str(path))
    tar_tag_info.size = len(encoded_data)
    tar_dump.addfile(tar_tag_info, data_buffer)


def main():
    tar_dump = tarfile.TarFile("medialib-dump.tar", "w")
    tag_uniq_id: dict[TagUnique, int] = dict()

    sql_get_content = "SELECT * FROM content order by RANDOM()"
    sql_get_tag_ids = "SELECT tag_id FROM content_tags_list WHERE content_id=%s"
    sql_get_tag = "SELECT * FROM tag WHERE ID=%s"
    sql_get_tag_aliases = "SELECT title FROM tag_alias WHERE tag_id=%s"
    sql_get_image_hash = (
        "SELECT aspect_ratio, ENCODE(value_hash, 'hex'), hue_hash, saturation_hash, alternate_version "
        "FROM imagehash WHERE content_id = %s"
    )
    sql_get_representations = (
        "SELECT * FROM representations WHERE content_id = %s"
    )
    sql_get_album = (
        "SELECT album.set_tag_id, album.album_artist_tag_id, album_order.order "
        "FROM album_order JOIN album ON album_order.album_id = album.id "
        "WHERE album_order.content_id = %s"
    )
    sql_get_alternate_sources = "SELECT origin, origin_content_id FROM alternate_sources WHERE content_id = %s"

    connection = common.make_connection()
    cursor = connection.cursor()

    cursor.execute(sql_get_content, tuple())
    raw_content_data = cursor.fetchall()

    def tags_processing(tag_id, cursor) -> TagUnique:
        global tags_uniq
        global tag_documents

        cursor.execute(sql_get_tag, (tag_id,))
        tag_raw_data = cursor.fetchone()
        tag_uniq = TagUnique(tag_raw_data[1], tag_raw_data[2])
        if tag_uniq not in tags_uniq:
            parent_tag_uniq = None
            if tag_raw_data[3] is not None:
                parent_tag_uniq = tags_processing(tag_raw_data[3], cursor)
            cursor.execute(sql_get_tag_aliases, (tag_raw_data[0],))
            tag_aliases = set([raw_alias[0] for raw_alias in cursor.fetchall()])
            tag_document = TagDocument(
                title=tag_raw_data[1],
                category=tag_raw_data[2],
                aliases=tag_aliases,
                parent=parent_tag_uniq
            )
            tag_uniq_id[tag_uniq] = tag_id
            tag_document_io = io.StringIO()
            json.dump(tag_document.json_serializable(), tag_document_io)
            tag_document_path = pathlib.PurePath("tags/{}.json".format(tag_id))
            create_tar_file(tar_dump, tag_document_io, tag_document_path)
            tags_uniq.add(tag_uniq)
        return tag_uniq

    for content in raw_content_data:
        tags: set[TagUnique] = set()
        content_id = content[0]
        alternate_sources: list[AlternateSource] = []
        cursor.execute(sql_get_tag_ids, (content_id,))
        tag_ids = cursor.fetchall()
        for tag_id_wrapped in tag_ids:
            tags.add(tags_processing(tag_id_wrapped[0], cursor))
        image_hash = None
        cursor.execute(sql_get_image_hash, (content_id,))
        image_hash_raw = cursor.fetchall()
        if image_hash_raw is not None and len(image_hash_raw):
            image_hash = ImageHash(
                image_hash_raw[0][0],
                image_hash_raw[0][1],
                image_hash_raw[0][2],
                image_hash_raw[0][3],
                image_hash_raw[0][4]
            )
        representations = None
        cursor.execute(sql_get_representations, (content_id,))
        raw_representations = cursor.fetchall()
        if raw_representations is not None and len(raw_representations):
            representations = list()
            for representations_raw_item in raw_representations:
                representations.append(ContentRepresentationElement(
                    representations_raw_item[1],
                    representations_raw_item[2],
                    pathlib.Path(representations_raw_item[3])
                ))
        albums = None
        cursor.execute(sql_get_album, (content_id,))
        raw_albums = cursor.fetchall()
        if raw_albums is not None and len(raw_albums):
            albums = list()
            for raw_album_item in raw_albums:
                albums.append(AlbumOrder(
                    tags_processing(raw_album_item[0], cursor),
                    tags_processing(raw_album_item[1], cursor),
                    raw_album_item[2]
                ))
        cursor.execute(sql_get_alternate_sources, (content_id,))
        alt_src = cursor.fetchone()
        while alt_src is not None:
            alternate_sources.append(AlternateSource(alt_src[0], alt_src[1]))

            alt_src = cursor.fetchone()

        content_document = ContentDocument(
            content_id=content_id,
            file_path=pathlib.Path(content[1]),
            title=content[2],
            content_type=content[3],
            description=content[4],
            addition_date=content[5],
            origin=content[6],
            origin_content_id=content[7],
            is_hidden=content[8],
            tags=tags,
            imagehash=image_hash,
            representations=representations,
            albums=albums,
            alternate_sources=alternate_sources
        )

        content_document_io = io.StringIO()
        json.dump(content_document.json_serializable(), content_document_io)
        content_document_path = pathlib.PurePath("content-metadata/{}.json".format(content_id))

        try:
            if content_document.file_path.suffix == ".srs":
                write_srs(tar_dump, content_document.file_path, content_id)
            elif content_document.file_path.suffix == ".mpd":
                write_mpd(tar_dump, content_document.file_path, content_id)
            else:
                write_regular(tar_dump, content_document.file_path, content_id)
        except FileNotFoundError as e:
            print("Missing file \"{}\", content id = {}!".format(e.filename, content_id))
        except xml.parsers.expat.ExpatError as e:
            print("Invalid MPD file, content id = {}!".format(content_id))
        else:
            create_tar_file(tar_dump, content_document_io, content_document_path)
    tag_uniq_id_io = io.StringIO()
    tag_uniq_id_serialisable = []
    for elem in tag_uniq_id:
        _tag_uniq = dataclasses.astuple(elem)
        tag_uniq_id_serialisable.append((_tag_uniq[0], _tag_uniq[1], tag_uniq_id[elem]))
    json.dump(tag_uniq_id_serialisable, tag_uniq_id_io)
    tag_uniq_id_filepath = pathlib.PurePath("tag_uniq_id.json")
    create_tar_file(tar_dump, tag_uniq_id_io, tag_uniq_id_filepath)

    checksums_io = io.StringIO()
    for checksum in checksums:
        checksums_io.write("{} {}\n".format(hex(checksum[0]).replace("0x", ""), checksum[1]))
    checksums_filepath = pathlib.PurePath("checksums-crc64")
    create_tar_file(tar_dump, checksums_io, checksums_filepath, write_crc=False)

    tar_dump.close()
    connection.close()


if __name__ == "__main__":
    main()
